using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Extensions;
using DistributedRateLimiter.Core.Model;
using Microsoft.Data.SqlClient;

namespace DistributedRateLimiter.Providers.MsSql;

public sealed class MsSqlRateLimitStore : IRateLimitStore
{
    private readonly string _connectionString;
    private readonly string _tableName;

    public MsSqlRateLimitStore(string connectionString, string tableName = "__rate_limits")
    {
        _connectionString = connectionString;
        _tableName = tableName;
    }

    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF OBJECT_ID(N'[{_tableName}]', N'U') IS NULL
            CREATE TABLE [{_tableName}] (
                [key]        NVARCHAR(512)  NOT NULL,
                window_start DATETIMEOFFSET NOT NULL,
                count        INT            NOT NULL DEFAULT 0,
                tokens       FLOAT          NULL,
                last_refill  DATETIMEOFFSET NULL,
                PRIMARY KEY ([key], window_start)
            );
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = N'idx_{_tableName}_cleanup'
                  AND object_id = OBJECT_ID(N'[{_tableName}]')
            )
            CREATE INDEX [idx_{_tableName}_cleanup]
                ON [{_tableName}] (window_start);
            """;

        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<RateLimitResult> SlidingWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        using var tx = conn.BeginTransaction();

        await using var lockCmd = conn.CreateCommand();
        lockCmd.Transaction = tx;
        lockCmd.CommandText = """
            DECLARE @res INT;
            EXEC @res = sp_getapplock @Resource=@lockResource, @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout=30000;
            IF @res < 0 RAISERROR('Failed to acquire rate limit lock', 16, 1);
            """;
        lockCmd.Parameters.Add(new SqlParameter("lockResource", key));
        await lockCmd.ExecuteNonQueryAsync(ct);

        await using var upsertCmd = conn.CreateCommand();
        upsertCmd.Transaction = tx;
        upsertCmd.CommandText = $"""
            DECLARE @ts DATETIME2 = SYSUTCDATETIME();
            MERGE [{_tableName}] WITH (HOLDLOCK) AS target
            USING (SELECT @key AS [key], @ts AS window_start) AS source
                ON target.[key] = source.[key] AND target.window_start = source.window_start
            WHEN MATCHED THEN
                UPDATE SET count = target.count + 1
            WHEN NOT MATCHED THEN
                INSERT ([key], window_start, count) VALUES (@key, @ts, 1);
            """;
        upsertCmd.Parameters.Add(new SqlParameter("key", key));
        await upsertCmd.ExecuteNonQueryAsync(ct);

        await using var countCmd = conn.CreateCommand();
        countCmd.Transaction = tx;
        countCmd.CommandText = $"""
            SELECT COALESCE(SUM(count), 0) FROM [{_tableName}]
            WHERE [key] = @key
              AND window_start > DATEADD(SECOND, -@windowSeconds, SYSUTCDATETIME());
            """;
        countCmd.Parameters.Add(new SqlParameter("key", key));
        countCmd.Parameters.Add(new SqlParameter("windowSeconds", (int)window.TotalSeconds));

        var count = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));
        await tx.CommitAsync(ct);

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

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            DECLARE @result TABLE (count INT);
            MERGE [{_tableName}] WITH (HOLDLOCK) AS target
            USING (SELECT @key AS [key], @windowStart AS window_start) AS source
                ON target.[key] = source.[key] AND target.window_start = source.window_start
            WHEN MATCHED THEN
                UPDATE SET count = target.count + 1
            WHEN NOT MATCHED THEN
                INSERT ([key], window_start, count) VALUES (@key, @windowStart, 1)
            OUTPUT INSERTED.count INTO @result;
            SELECT count FROM @result;
            """;

        cmd.Parameters.Add(new SqlParameter("key", key));
        cmd.Parameters.Add(new SqlParameter("windowStart", windowStart));

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
        // Sentinel window_start separates token bucket rows from window rows
        var sentinel = new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero);

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            DECLARE @result TABLE (tokens FLOAT);
            MERGE [{_tableName}] WITH (HOLDLOCK) AS target
            USING (SELECT @key AS [key], @sentinel AS window_start) AS source
                ON target.[key] = source.[key] AND target.window_start = source.window_start
            WHEN MATCHED THEN
                UPDATE SET
                    tokens = GREATEST(-1,
                        LEAST(
                            CAST(@capacity AS FLOAT),
                            target.tokens +
                            DATEDIFF(MILLISECOND, target.last_refill, SYSUTCDATETIME())
                            / 1000.0 * @refillRate
                        ) - 1),
                    last_refill = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT ([key], window_start, tokens, last_refill)
                VALUES (@key, @sentinel, @capacity - 1, SYSUTCDATETIME())
            OUTPUT INSERTED.tokens INTO @result;
            SELECT tokens FROM @result;
            """;

        cmd.Parameters.Add(new SqlParameter("key", key));
        cmd.Parameters.Add(new SqlParameter("sentinel", sentinel));
        cmd.Parameters.Add(new SqlParameter("capacity", capacity));
        cmd.Parameters.Add(new SqlParameter("refillRate", refillRatePerSecond));

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
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        var sentinel = new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero);
        cmd.CommandText =
            $"DELETE FROM [{_tableName}] WHERE window_start < DATEADD(SECOND, -@maxAgeSeconds, SYSUTCDATETIME()) AND window_start != @sentinel";
        cmd.Parameters.Add(new SqlParameter("maxAgeSeconds", (int)maxAge.TotalSeconds));
        cmd.Parameters.Add(new SqlParameter("sentinel", sentinel));

        await cmd.ExecuteNonQueryAsync(ct);
    }
}