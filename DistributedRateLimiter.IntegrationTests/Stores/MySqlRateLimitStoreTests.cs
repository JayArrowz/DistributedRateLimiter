using DistributedRateLimiter.Providers.MySql;
using DistributedRateLimiter.Core.Interface;
using MySqlConnector;
using Xunit;

namespace DistributedRateLimiter.IntegrationTests.Stores;

[Trait("Provider", "MySql")]
public sealed class MySqlRateLimitStoreTests : RateLimitStoreTests
{
    protected override string? ConnectionString =>
        Environment.GetEnvironmentVariable("MYSQL_CONNECTION_STRING");

    protected override string ProviderName => "MySQL";

    protected override IRateLimitStore CreateStore(string connectionString, string tableName) =>
        new MySqlRateLimitStore(connectionString, tableName);

    protected override async Task DropTestTableAsync(string connectionString, string tableName)
    {
        await using var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS `{tableName}`";
        await cmd.ExecuteNonQueryAsync();
    }

    protected override async Task InsertRowDirectlyAsync(
        string connectionString, string tableName, string key, DateTimeOffset windowStart)
    {
        await using var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        // DATETIME(6) has no timezone; pass UTC DateTime explicitly.
        cmd.CommandText = $"""
            INSERT INTO `{tableName}` (`key`, `window_start`, `count`)
            VALUES (@key, @ts, 1)
            ON DUPLICATE KEY UPDATE `count` = `count` + 1;
            """;
        cmd.Parameters.AddWithValue("key", key);
        cmd.Parameters.AddWithValue("ts", windowStart.UtcDateTime);
        await cmd.ExecuteNonQueryAsync();
    }
}
