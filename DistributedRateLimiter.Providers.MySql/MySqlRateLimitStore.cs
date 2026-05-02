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

        await using var tableCmd = conn.CreateCommand();
        tableCmd.CommandText = $"""
            CREATE TABLE IF NOT EXISTS `{_tableName}` (
                `key`          VARCHAR(512) NOT NULL,
                `window_start` DATETIME(6)  NOT NULL,
                `count`        INT          NOT NULL DEFAULT 0,
                `tokens`       DOUBLE       NULL,
                `last_refill`  DATETIME(6)  NULL,
                PRIMARY KEY (`key`, `window_start`)
            )
            """;
        await tableCmd.ExecuteNonQueryAsync(ct);

        await using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = """
            SELECT COUNT(*) FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = @tableName
              AND INDEX_NAME   = @indexName
            """;
        checkCmd.Parameters.AddWithValue("tableName", _tableName);
        checkCmd.Parameters.AddWithValue("indexName", $"idx_{_tableName}_cleanup");
        var indexExists = Convert.ToInt32(await checkCmd.ExecuteScalarAsync(ct)) > 0;

        if (!indexExists)
        {
            await using var idxCmd = conn.CreateCommand();
            idxCmd.CommandText =
                $"CREATE INDEX `idx_{_tableName}_cleanup` ON `{_tableName}` (`window_start`)";
            await idxCmd.ExecuteNonQueryAsync(ct);
        }
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

        await using var tx = await conn.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, ct);

        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $"""
            INSERT INTO `{_tableName}` (`key`, `window_start`, `count`)
            VALUES (@key, UTC_TIMESTAMP(), 1)
            ON DUPLICATE KEY UPDATE `count` = `count` + 1;
            SELECT COALESCE(SUM(`count`), 0), MIN(`window_start`) FROM `{_tableName}`
            WHERE `key` = @key
              AND `window_start` > DATE_SUB(UTC_TIMESTAMP(), INTERVAL @windowSeconds SECOND);
            """;
        cmd.Parameters.AddWithValue("key", key);
        cmd.Parameters.AddWithValue("windowSeconds", windowSeconds);

        int count;
        DateTimeOffset? oldestWindowStart;
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            await reader.ReadAsync(ct);
            count = Convert.ToInt32(reader[0]);
            oldestWindowStart = reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTimeOffset>(1);
        }
        await tx.CommitAsync(ct);

        var retryAfter = count > limit && oldestWindowStart.HasValue
            ? oldestWindowStart.Value + window - DateTimeOffset.UtcNow
            : TimeSpan.Zero;

        return new RateLimitResult(
            Allowed: count <= limit,
            Remaining: Math.Max(0, limit - count),
            Limit: limit,
            RetryAfter: retryAfter);
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
        await using var tx = await conn.BeginTransactionAsync(ct);

        await using var upsertCmd = conn.CreateCommand();
        upsertCmd.Transaction = tx;
        upsertCmd.CommandText = $"""
            INSERT INTO `{_tableName}` (`key`, `window_start`, `count`)
            VALUES (@key, @windowStart, 1)
            ON DUPLICATE KEY UPDATE `count` = `count` + 1;
            """;
        upsertCmd.Parameters.AddWithValue("key", key);
        upsertCmd.Parameters.AddWithValue("windowStart", windowStart);
        await upsertCmd.ExecuteNonQueryAsync(ct);

        await using var selectCmd = conn.CreateCommand();
        selectCmd.Transaction = tx;
        selectCmd.CommandText = $"""
            SELECT `count` FROM `{_tableName}`
            WHERE `key` = @key AND `window_start` = @windowStart
            FOR UPDATE;
            """;
        selectCmd.Parameters.AddWithValue("key", key);
        selectCmd.Parameters.AddWithValue("windowStart", windowStart);
        var count = (int)(await selectCmd.ExecuteScalarAsync(ct))!;

        await tx.CommitAsync(ct);

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
        var sentinel = new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        await using var upsertCmd = conn.CreateCommand();
        upsertCmd.Transaction = tx;
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

        await using var selectCmd = conn.CreateCommand();
        selectCmd.Transaction = tx;
        selectCmd.CommandText = $"""
            SELECT `tokens` FROM `{_tableName}`
            WHERE `key` = @key AND `window_start` = @sentinel
            FOR UPDATE;
            """;
        selectCmd.Parameters.AddWithValue("key", key);
        selectCmd.Parameters.AddWithValue("sentinel", sentinel);
        var tokens = (double)(await selectCmd.ExecuteScalarAsync(ct))!;

        await tx.CommitAsync(ct);

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
        var sentinel = new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        cmd.CommandText =
            $"DELETE FROM `{_tableName}` WHERE `window_start` < DATE_SUB(UTC_TIMESTAMP(6), INTERVAL @maxAgeSeconds SECOND) AND `window_start` != @sentinel";
        cmd.Parameters.AddWithValue("maxAgeSeconds", (int)maxAge.TotalSeconds);
        cmd.Parameters.AddWithValue("sentinel", sentinel);

        await cmd.ExecuteNonQueryAsync(ct);
    }
}