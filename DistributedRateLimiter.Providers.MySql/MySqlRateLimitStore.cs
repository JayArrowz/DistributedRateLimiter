using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;
using MySqlConnector;
using DistributedRateLimiter.Core.Extensions;

namespace DistributedRateLimiter.Providers.MySql;

public sealed class MySqlRateLimitStore : IRateLimitStore
{
    private readonly string _connectionString;
    private readonly string _tableName;

    public MySqlRateLimitStore(string connectionString, string tableName = "__rate_limits")
    {
        _connectionString = connectionString;
        _tableName = tableName;
    }

    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            CREATE TABLE IF NOT EXISTS `{_tableName}` (
                `key`          VARCHAR(512) NOT NULL,
                `window_start` DATETIME(6)  NOT NULL,
                `count`        INT          NOT NULL DEFAULT 0,
                `tokens`       DOUBLE       NULL,
                `last_refill`  DATETIME(6)  NULL,
                PRIMARY KEY (`key`, `window_start`)
            );
            CREATE INDEX IF NOT EXISTS `idx_{_tableName}_cleanup`
                ON `{_tableName}` (`window_start`);
            """;

        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<RateLimitResult> SlidingWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken ct = default)
    {
        var windowSeconds = (int)window.TotalSeconds;

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // MySQL lacks RETURNING on upserts — two round-trips required
        await using (var upsertCmd = conn.CreateCommand())
        {
            upsertCmd.CommandText = $"""
                INSERT INTO `{_tableName}` (`key`, `window_start`, `count`)
                VALUES (
                    @key,
                    FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) / @windowSeconds) * @windowSeconds),
                    1
                )
                ON DUPLICATE KEY UPDATE `count` = `count` + 1;
                """;

            upsertCmd.Parameters.AddWithValue("key", key);
            upsertCmd.Parameters.AddWithValue("windowSeconds", windowSeconds);

            await upsertCmd.ExecuteNonQueryAsync(ct);
        }

        await using var selectCmd = conn.CreateCommand();
        selectCmd.CommandText = $"""
            SELECT `count` FROM `{_tableName}`
            WHERE `key` = @key
              AND `window_start` = FROM_UNIXTIME(
                  FLOOR(UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) / @windowSeconds) * @windowSeconds
              );
            """;

        selectCmd.Parameters.AddWithValue("key", key);
        selectCmd.Parameters.AddWithValue("windowSeconds", windowSeconds);

        var count = (int)(await selectCmd.ExecuteScalarAsync(ct))!;

        return new RateLimitResult(
            Allowed: count <= limit,
            Remaining: Math.Max(0, limit - count),
            Limit: limit,
            RetryAfter: count > limit ? window : TimeSpan.Zero);
    }

    public async Task<RateLimitResult> FixedWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken ct = default)
    {
        var windowStart = window.GetFixedWindowStart();

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using (var upsertCmd = conn.CreateCommand())
        {
            upsertCmd.CommandText = $"""
                INSERT INTO `{_tableName}` (`key`, `window_start`, `count`)
                VALUES (@key, @windowStart, 1)
                ON DUPLICATE KEY UPDATE `count` = `count` + 1;
                """;

            upsertCmd.Parameters.AddWithValue("key", key);
            upsertCmd.Parameters.AddWithValue("windowStart", windowStart);

            await upsertCmd.ExecuteNonQueryAsync(ct);
        }

        await using var selectCmd = conn.CreateCommand();
        selectCmd.CommandText = $"""
            SELECT `count` FROM `{_tableName}`
            WHERE `key` = @key AND `window_start` = @windowStart;
            """;

        selectCmd.Parameters.AddWithValue("key", key);
        selectCmd.Parameters.AddWithValue("windowStart", windowStart);

        var count = (int)(await selectCmd.ExecuteScalarAsync(ct))!;

        return new RateLimitResult(
            Allowed: count <= limit,
            Remaining: Math.Max(0, limit - count),
            Limit: limit,
            RetryAfter: count > limit ? window : TimeSpan.Zero);
    }

    public async Task<RateLimitResult> TokenBucketAsync(
        string key,
        int capacity,
        double refillRatePerSecond,
        CancellationToken ct = default)
    {
        // Sentinel window_start separates token bucket rows from window rows
        var sentinel = new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using (var upsertCmd = conn.CreateCommand())
        {
            upsertCmd.CommandText = $"""
                INSERT INTO `{_tableName}` (`key`, `window_start`, `tokens`, `last_refill`)
                VALUES (@key, @sentinel, @capacity - 1, UTC_TIMESTAMP(6))
                ON DUPLICATE KEY UPDATE
                    `tokens` = GREATEST(-1,
                        LEAST(
                            @capacity,
                            `tokens` + TIMESTAMPDIFF(MICROSECOND, `last_refill`, UTC_TIMESTAMP(6))
                            / 1000000.0 * @refillRate
                        ) - 1),
                    `last_refill` = UTC_TIMESTAMP(6);
                """;

            upsertCmd.Parameters.AddWithValue("key", key);
            upsertCmd.Parameters.AddWithValue("sentinel", sentinel);
            upsertCmd.Parameters.AddWithValue("capacity", capacity);
            upsertCmd.Parameters.AddWithValue("refillRate", refillRatePerSecond);

            await upsertCmd.ExecuteNonQueryAsync(ct);
        }

        await using var selectCmd = conn.CreateCommand();
        selectCmd.CommandText = $"""
            SELECT `tokens` FROM `{_tableName}`
            WHERE `key` = @key AND `window_start` = @sentinel;
            """;

        selectCmd.Parameters.AddWithValue("key", key);
        selectCmd.Parameters.AddWithValue("sentinel", sentinel);

        var tokens = (double)(await selectCmd.ExecuteScalarAsync(ct))!;
        var allowed = tokens >= 0;

        return new RateLimitResult(
            Allowed: allowed,
            Remaining: (int)Math.Max(0, Math.Floor(tokens)),
            Limit: capacity,
            RetryAfter: allowed
                ? TimeSpan.Zero
                : TimeSpan.FromSeconds(1.0 / refillRatePerSecond));
    }

    public async Task PurgeExpiredAsync(TimeSpan maxAge, CancellationToken ct = default)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"DELETE FROM `{_tableName}` WHERE `window_start` < DATE_SUB(UTC_TIMESTAMP(6), INTERVAL @maxAgeSeconds SECOND)";
        cmd.Parameters.AddWithValue("maxAgeSeconds", (int)maxAge.TotalSeconds);

        await cmd.ExecuteNonQueryAsync(ct);
    }
}