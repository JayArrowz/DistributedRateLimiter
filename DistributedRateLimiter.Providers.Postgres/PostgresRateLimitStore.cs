using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Extensions;
using DistributedRateLimiter.Core.Model;
using Npgsql;

namespace DistributedRateLimiter.Providers.Postgres;

public sealed class PostgresRateLimitStore : IRateLimitStore
{
    private readonly string _connectionString;
    private readonly string _tableName;

    public PostgresRateLimitStore(string connectionString, string tableName = "__rate_limits")
    {
        _connectionString = connectionString;
        _tableName = tableName;
    }

    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            CREATE TABLE IF NOT EXISTS {_tableName} (
                key          TEXT        NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                count        INT         NOT NULL DEFAULT 0,
                tokens       FLOAT       NULL,
                last_refill  TIMESTAMPTZ NULL,
                PRIMARY KEY (key, window_start)
            );
            CREATE INDEX IF NOT EXISTS idx_{_tableName}_cleanup
                ON {_tableName} (window_start);
            """;

        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<RateLimitResult> SlidingWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        
        await using var tx = await conn.BeginTransactionAsync(ct);

        await using var upsertCmd = conn.CreateCommand();
        upsertCmd.Transaction = tx;
        upsertCmd.CommandText = $"""
            INSERT INTO {_tableName} (key, window_start, count)
            VALUES (@key, DATE_TRUNC('second', clock_timestamp()), 1)
            ON CONFLICT (key, window_start)
                DO UPDATE SET count = {_tableName}.count + 1;
            """;
        upsertCmd.Parameters.AddWithValue("key", key);
        await upsertCmd.ExecuteNonQueryAsync(ct);

        await using var countCmd = conn.CreateCommand();
        countCmd.Transaction = tx;
        countCmd.CommandText = $"""
            SELECT COALESCE(SUM(count), 0), MIN(window_start)
            FROM {_tableName}
            WHERE key = @key
              AND window_start > clock_timestamp() - @windowSeconds * interval '1 second';
            """;
        countCmd.Parameters.AddWithValue("key", key);
        countCmd.Parameters.AddWithValue("windowSeconds", (int)window.TotalSeconds);

        int count;
        DateTimeOffset? oldestWindowStart;
        await using (var reader = await countCmd.ExecuteReaderAsync(ct))
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

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            INSERT INTO {_tableName} (key, window_start, count)
            VALUES (@key, @windowStart, 1)
            ON CONFLICT (key, window_start)
                DO UPDATE SET count = {_tableName}.count + 1
            RETURNING count;
            """;

        cmd.Parameters.AddWithValue("key", key);
        cmd.Parameters.AddWithValue("windowStart", windowStart);

        var count = (int)(await cmd.ExecuteScalarAsync(ct))!;

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
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            INSERT INTO {_tableName} (key, window_start, tokens, last_refill)
            VALUES (@key, '-infinity', @capacity - 1, now())
            ON CONFLICT (key, window_start) DO UPDATE SET
                tokens = GREATEST(-1,
                    LEAST(@capacity,
                        {_tableName}.tokens +
                        EXTRACT(EPOCH FROM (now() - {_tableName}.last_refill))
                        * @refillRate
                    ) - 1),
                last_refill = now()
            RETURNING tokens;
            """;

        cmd.Parameters.AddWithValue("key", key);
        cmd.Parameters.AddWithValue("capacity", capacity);
        cmd.Parameters.AddWithValue("refillRate", refillRatePerSecond);

        var tokens = (double)(await cmd.ExecuteScalarAsync(ct))!;
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
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"DELETE FROM {_tableName} WHERE window_start < now() - @maxAge * interval '1 second' AND window_start != '-infinity'";
        cmd.Parameters.AddWithValue("maxAge", (int)maxAge.TotalSeconds);

        await cmd.ExecuteNonQueryAsync(ct);
    }
}