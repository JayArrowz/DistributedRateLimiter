using DistributedRateLimiter.Providers.MsSql;
using DistributedRateLimiter.Core.Interface;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DistributedRateLimiter.IntegrationTests.Stores;

[Trait("Provider", "MsSql")]
public sealed class MsSqlRateLimitStoreTests : RateLimitStoreTests
{
    protected override string? ConnectionString =>
        Environment.GetEnvironmentVariable("MSSQL_CONNECTION_STRING");

    protected override string ProviderName => "SQL Server";

    protected override IRateLimitStore CreateStore(string connectionString, string tableName) =>
        new MsSqlRateLimitStore(connectionString, tableName);

    protected override async Task DropTestTableAsync(string connectionString, string tableName)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS [{tableName}]";
        await cmd.ExecuteNonQueryAsync();
    }

    protected override async Task InsertRowDirectlyAsync(
        string connectionString, string tableName, string key, DateTimeOffset windowStart)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        // Use MERGE so a sub-millisecond collision folds into count rather than throwing.
        cmd.CommandText = $"""
            MERGE [{tableName}] WITH (HOLDLOCK) AS target
            USING (SELECT @key AS [key], @ts AS window_start) AS source
                ON target.[key] = source.[key] AND target.window_start = source.window_start
            WHEN MATCHED THEN
                UPDATE SET count = target.count + 1
            WHEN NOT MATCHED THEN
                INSERT ([key], window_start, count) VALUES (@key, @ts, 1);
            """;
        cmd.Parameters.Add(new SqlParameter("key", key));
        cmd.Parameters.Add(new SqlParameter("ts", windowStart));
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// A bucket idle for longer than ~24.8 days overflows an INT millisecond DATEDIFF,
    /// so the refill must use DATEDIFF_BIG.
    /// </summary>
    [SkippableFact]
    public async Task TokenBucket_LongIdleBucket_RefillsWithoutOverflow()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int capacity = 3;

        for (var i = 0; i < capacity; i++)
            await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 1);

        await using (var conn = new SqlConnection(ConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                UPDATE [{TableName}]
                SET last_refill = DATEADD(DAY, -30, SYSUTCDATETIME())
                WHERE [key] = @key AND tokens IS NOT NULL
                """;
            cmd.Parameters.Add(new SqlParameter("key", key));
            await cmd.ExecuteNonQueryAsync();
        }

        var r = await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 1);
        Assert.True(r.Allowed);
        Assert.Equal(capacity - 1, r.Remaining);
    }
}
