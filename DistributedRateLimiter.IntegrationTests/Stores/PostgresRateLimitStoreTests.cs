using DistributedRateLimiter.Providers.Postgres;
using DistributedRateLimiter.Core.Interface;
using Npgsql;
using Xunit;

namespace DistributedRateLimiter.IntegrationTests.Stores;

[Trait("Provider", "Postgres")]
public sealed class PostgresRateLimitStoreTests : RateLimitStoreTests
{
    protected override string? ConnectionString =>
        Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");

    protected override string ProviderName => "PostgreSQL";

    protected override IRateLimitStore CreateStore(string connectionString, string tableName) =>
        new PostgresRateLimitStore(connectionString, tableName);

    protected override async Task DropTestTableAsync(string connectionString, string tableName)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS {tableName}";
        await cmd.ExecuteNonQueryAsync();
    }

    protected override async Task InsertRowDirectlyAsync(
        string connectionString, string tableName, string key, DateTimeOffset windowStart)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            INSERT INTO {tableName} (key, window_start, count)
            VALUES (@key, @ts, 1)
            ON CONFLICT (key, window_start) DO UPDATE SET count = {tableName}.count + 1;
            """;
        cmd.Parameters.AddWithValue("key", key);
        cmd.Parameters.AddWithValue("ts", windowStart);
        await cmd.ExecuteNonQueryAsync();
    }
}
