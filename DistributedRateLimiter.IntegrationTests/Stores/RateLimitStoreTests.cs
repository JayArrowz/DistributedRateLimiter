using DistributedRateLimiter.Core.Interface;
using Xunit;

namespace DistributedRateLimiter.IntegrationTests.Stores;

/// <summary>
/// Abstract base for <see cref="IRateLimitStore"/> integration tests.
/// Each concrete subclass targets a specific database provider and is selected
/// in CI via the <c>Provider</c> trait (e.g. <c>--filter "Provider=Postgres"</c>).
/// </summary>
public abstract class RateLimitStoreTests : IAsyncLifetime
{
    // Unique table per test-class instance so parallel runs never collide.
    private readonly string _tableName = $"rl_test_{Guid.NewGuid():N}";

    protected abstract string? ConnectionString { get; }
    protected abstract string ProviderName { get; }
    protected abstract IRateLimitStore CreateStore(string connectionString, string tableName);
    protected abstract Task DropTestTableAsync(string connectionString, string tableName);

    /// <summary>
    /// Directly inserts a single row with the given <paramref name="windowStart"/> timestamp.
    /// Used to backdate rows so tests can exercise sliding-window boundary conditions
    /// without sleeping.
    /// </summary>
    protected abstract Task InsertRowDirectlyAsync(
        string connectionString, string tableName, string key, DateTimeOffset windowStart);

    protected IRateLimitStore Store { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        if (ConnectionString is null) return;
        Store = CreateStore(ConnectionString, _tableName);
        await Store.EnsureSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        if (ConnectionString is null) return;
        await DropTestTableAsync(ConnectionString, _tableName);
    }

    private void SkipIfUnavailable() =>
        Skip.If(ConnectionString is null,
            $"{ProviderName} connection string not set. " +
            "Provide it via the appropriate environment variable.");

    private static string Unique() => $"key_{Guid.NewGuid():N}";

    [SkippableFact]
    public async Task EnsureSchemaAsync_IsIdempotent()
    {
        SkipIfUnavailable();
        // Should not throw when called a second time (CREATE TABLE IF NOT EXISTS).
        await Store.EnsureSchemaAsync();
    }

    [SkippableFact]
    public async Task SlidingWindow_AllowsRequestsWithinLimit()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 5;

        for (var i = 1; i <= limit; i++)
        {
            var r = await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));
            Assert.True(r.Allowed, $"Request {i} should be allowed");
            Assert.Equal(limit, r.Limit);
        }
    }

    [SkippableFact]
    public async Task SlidingWindow_DeniesRequestOverLimit()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 3;

        for (var i = 0; i < limit; i++)
            await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));

        var r = await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.False(r.Allowed);
        Assert.Equal(0, r.Remaining);
        Assert.True(r.RetryAfter > TimeSpan.Zero);
    }

    [SkippableFact]
    public async Task SlidingWindow_RemainingDecrementsCorrectly()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 5;

        var first = await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.Equal(limit - 1, first.Remaining);

        var second = await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.Equal(limit - 2, second.Remaining);
    }

    [SkippableFact]
    public async Task SlidingWindow_RetryAfter_IsWithinWindowWhenDenied()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 2;
        var window = TimeSpan.FromMinutes(1);

        for (var i = 0; i < limit; i++)
            await Store.SlidingWindowAsync(key, limit, window);

        var r = await Store.SlidingWindowAsync(key, limit, window);
        Assert.False(r.Allowed);
        Assert.True(r.RetryAfter > TimeSpan.Zero, "RetryAfter must be positive when denied");
        Assert.True(r.RetryAfter <= window, "RetryAfter must not exceed the window duration");
    }

    [SkippableFact]
    public async Task SlidingWindow_RetryAfterIsZeroWhenAllowed()
    {
        SkipIfUnavailable();
        var key = Unique();
        var r = await Store.SlidingWindowAsync(key, 5, TimeSpan.FromMinutes(1));
        Assert.True(r.Allowed);
        Assert.Equal(TimeSpan.Zero, r.RetryAfter);
    }

    /// <summary>
    /// Inserts the oldest in-window row at -30 s into a 60-second window, then
    /// denies a request and verifies <see cref="RateLimitResult.RetryAfter"/> is
    /// meaningfully shorter than the full window — proving accurate expiry calculation.
    /// </summary>
    [SkippableFact]
    public async Task SlidingWindow_RetryAfter_IsShorterThanWindowWhenOldestRequestIsMidWindow()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 2;
        var window = TimeSpan.FromSeconds(60);

        // Oldest request is 30 seconds old — still inside the window.
        var backdated = DateTimeOffset.UtcNow - TimeSpan.FromSeconds(30);
        await InsertRowDirectlyAsync(ConnectionString!, _tableName, key, backdated);

        // Fill the remaining slot with a live request.
        await Store.SlidingWindowAsync(key, limit, window);

        // This request is denied. RetryAfter should be ~30 s, not the full 60 s.
        var r = await Store.SlidingWindowAsync(key, limit, window);
        Assert.False(r.Allowed);
        Assert.True(r.RetryAfter > TimeSpan.Zero, "RetryAfter must be positive when denied");
        Assert.True(r.RetryAfter < window, "RetryAfter must be less than the full window when oldest request is mid-window");
    }

    /// <summary>
    /// Fires <c>concurrency</c> simultaneous requests against a single key and
    /// verifies that exactly <c>limit</c> are allowed — proving the advisory lock
    /// serialises writes and prevents double-counting under concurrency.
    /// </summary>
    [SkippableFact]
    public async Task SlidingWindow_ConcurrentRequests_OnlyAllowedUpToLimit()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 5;
        const int concurrency = 20;

        var tasks = Enumerable.Range(0, concurrency)
            .Select(_ => Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1)))
            .ToList();

        var results = await Task.WhenAll(tasks);

        var allowed = results.Count(r => r.Allowed);
        Assert.Equal(limit, allowed);
    }

    [SkippableFact]
    public async Task SlidingWindow_KeysAreIsolated()
    {
        SkipIfUnavailable();
        var keyA = Unique();
        var keyB = Unique();
        const int limit = 1;

        // Drain keyA
        await Store.SlidingWindowAsync(keyA, limit, TimeSpan.FromMinutes(1));
        var denied = await Store.SlidingWindowAsync(keyA, limit, TimeSpan.FromMinutes(1));
        Assert.False(denied.Allowed);

        // keyB must be unaffected
        var r = await Store.SlidingWindowAsync(keyB, limit, TimeSpan.FromMinutes(1));
        Assert.True(r.Allowed, "keyB should be unaffected by keyA being rate-limited");
    }

    /// <summary>
    /// Injects rows whose timestamps are OUTSIDE the window via <see cref="InsertRowDirectlyAsync"/>
    /// and proves they are NOT counted — this verifies the window is truly time-based,
    /// not an ever-growing counter.
    /// </summary>
    [SkippableFact]
    public async Task SlidingWindow_DoesNotCountRequestsOutsideWindow()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 3;
        var window = TimeSpan.FromSeconds(10);

        // Insert `limit` rows whose timestamps are 11 seconds in the past — just outside the window.
        var outsideWindow = DateTimeOffset.UtcNow - TimeSpan.FromSeconds(11);
        for (var i = 0; i < limit; i++)
            await InsertRowDirectlyAsync(ConnectionString!, _tableName, key, outsideWindow.AddMilliseconds(i));

        // Only the current request should be counted (the backdated ones are out of window).
        var r = await Store.SlidingWindowAsync(key, limit, window);
        Assert.True(r.Allowed, "Outdated rows must not be counted by the sliding window");
        Assert.Equal(limit - 1, r.Remaining); // count=1, remaining=limit-1
    }

    /// <summary>
    /// Injects rows whose timestamps are INSIDE the window via <see cref="InsertRowDirectlyAsync"/>
    /// and proves they ARE counted — confirming the window is a proper rolling lookback.
    /// </summary>
    [SkippableFact]
    public async Task SlidingWindow_CountsRequestsInsideWindow()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 3;
        var window = TimeSpan.FromSeconds(10);

        // Insert `limit` rows 5 seconds ago — inside the 10-second window.
        var insideWindow = DateTimeOffset.UtcNow - TimeSpan.FromSeconds(5);
        for (var i = 0; i < limit; i++)
            await InsertRowDirectlyAsync(ConnectionString!, _tableName, key, insideWindow.AddMilliseconds(i));

        // Adding one more makes limit+1 requests in the window → denied.
        var r = await Store.SlidingWindowAsync(key, limit, window);
        Assert.False(r.Allowed, "Recent backdated rows must be counted in the sliding window");
        Assert.Equal(0, r.Remaining);
    }

    [SkippableFact]
    public async Task FixedWindow_AllowsRequestsWithinLimit()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 5;

        for (var i = 1; i <= limit; i++)
        {
            var r = await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));
            Assert.True(r.Allowed, $"Request {i} should be allowed");
            Assert.Equal(limit, r.Limit);
        }
    }

    [SkippableFact]
    public async Task FixedWindow_DeniesRequestOverLimit()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 3;

        for (var i = 0; i < limit; i++)
            await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));

        var r = await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.False(r.Allowed);
        Assert.Equal(0, r.Remaining);
        Assert.True(r.RetryAfter > TimeSpan.Zero);
    }

    [SkippableFact]
    public async Task FixedWindow_RetryAfterIsZeroWhenAllowed()
    {
        SkipIfUnavailable();
        var key = Unique();
        var r = await Store.FixedWindowAsync(key, 5, TimeSpan.FromMinutes(1));
        Assert.True(r.Allowed);
        Assert.Equal(TimeSpan.Zero, r.RetryAfter);
    }

    [SkippableFact]
    public async Task FixedWindow_RemainingDecrementsCorrectly()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 5;

        var first = await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.Equal(limit - 1, first.Remaining);

        var second = await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.Equal(limit - 2, second.Remaining);
    }

    [SkippableFact]
    public async Task FixedWindow_RetryAfter_IsWindowDurationWhenDenied()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 2;
        var window = TimeSpan.FromMinutes(1);

        for (var i = 0; i < limit; i++)
            await Store.FixedWindowAsync(key, limit, window);

        var r = await Store.FixedWindowAsync(key, limit, window);
        Assert.False(r.Allowed);
        Assert.Equal(window, r.RetryAfter);
    }

    /// <summary>
    /// Inserts rows into the PREVIOUS bucket (epoch-aligned), then calls <see cref="IRateLimitStore.FixedWindowAsync"/>
    /// and verifies the count resets to 1 — proving the window is epoch-aligned, not an ever-growing counter.
    /// </summary>
    [SkippableFact]
    public async Task FixedWindow_WindowBoundary_ResetsCountAtBoundary()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 3;
        var window = TimeSpan.FromMinutes(1);

        // Compute the start of the previous bucket using the same epoch-division logic as GetFixedWindowStart.
        var windowSeconds = (long)window.TotalSeconds;
        var unixNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var currentWindowStart = DateTimeOffset.FromUnixTimeSeconds(unixNow / windowSeconds * windowSeconds);
        var prevWindowStart = currentWindowStart - window;

        // Fill the previous window completely so it would be over-limit if counted.
        for (var i = 0; i < limit; i++)
            await InsertRowDirectlyAsync(ConnectionString!, _tableName, key, prevWindowStart.AddMilliseconds(i));

        // The current window is a fresh bucket — this must be the first request in it.
        var r = await Store.FixedWindowAsync(key, limit, window);
        Assert.True(r.Allowed, "Previous window rows must not count toward the current window");
        Assert.Equal(limit - 1, r.Remaining);
    }

    [SkippableFact]
    public async Task FixedWindow_SubMinuteWindow_AllowsAndDeniesCorrectly()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 2;
        var window = TimeSpan.FromSeconds(10);

        var first = await Store.FixedWindowAsync(key, limit, window);
        Assert.True(first.Allowed);
        Assert.Equal(limit - 1, first.Remaining);

        var second = await Store.FixedWindowAsync(key, limit, window);
        Assert.True(second.Allowed);
        Assert.Equal(0, second.Remaining);

        var third = await Store.FixedWindowAsync(key, limit, window);
        Assert.False(third.Allowed);
        Assert.Equal(TimeSpan.FromSeconds(10), third.RetryAfter);
    }

    [SkippableFact]
    public async Task FixedWindow_KeysAreIsolated()
    {
        SkipIfUnavailable();
        var keyA = Unique();
        var keyB = Unique();
        const int limit = 1;

        await Store.FixedWindowAsync(keyA, limit, TimeSpan.FromMinutes(1));
        var denied = await Store.FixedWindowAsync(keyA, limit, TimeSpan.FromMinutes(1));
        Assert.False(denied.Allowed);

        var r = await Store.FixedWindowAsync(keyB, limit, TimeSpan.FromMinutes(1));
        Assert.True(r.Allowed, "keyB should be unaffected by keyA being rate-limited");
    }

    [SkippableFact]
    public async Task TokenBucket_AllowsRequestsWithinCapacity()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int capacity = 5;

        for (var i = 1; i <= capacity; i++)
        {
            var r = await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 1);
            Assert.True(r.Allowed, $"Request {i} should be allowed");
            Assert.Equal(capacity, r.Limit);
        }
    }

    [SkippableFact]
    public async Task TokenBucket_DeniesRequestOverCapacity()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int capacity = 3;

        for (var i = 0; i < capacity; i++)
            await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 0.0001);

        var r = await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 0.0001);
        Assert.False(r.Allowed);
        Assert.Equal(0, r.Remaining);
    }

    [SkippableFact]
    public async Task TokenBucket_RetryAfterIsZeroWhenAllowed()
    {
        SkipIfUnavailable();
        var key = Unique();
        var r = await Store.TokenBucketAsync(key, 5, refillRatePerSecond: 1);
        Assert.True(r.Allowed);
        Assert.Equal(TimeSpan.Zero, r.RetryAfter);
    }

    [SkippableFact]
    public async Task TokenBucket_RemainingDecrementsCorrectly()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int capacity = 5;

        // Negligible refill rate so the bucket doesn't top-up between calls.
        var first = await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 0.0001);
        Assert.True(first.Allowed);
        Assert.Equal(capacity - 1, first.Remaining);

        var second = await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 0.0001);
        Assert.True(second.Allowed);
        Assert.Equal(capacity - 2, second.Remaining);
    }

    [SkippableFact]
    public async Task TokenBucket_RetryAfter_IsOneOverRefillRateWhenDenied()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int capacity = 1;
        const double refillRate = 2.0; // 2 tokens/s → retry after 0.5 s

        await Store.TokenBucketAsync(key, capacity, refillRate); // consume the only token
        var r = await Store.TokenBucketAsync(key, capacity, refillRate);

        Assert.False(r.Allowed);
        Assert.Equal(TimeSpan.FromSeconds(1.0 / refillRate), r.RetryAfter);
    }

    [SkippableFact]
    public async Task TokenBucket_KeysAreIsolated()
    {
        SkipIfUnavailable();
        var keyA = Unique();
        var keyB = Unique();
        const int capacity = 1;

        await Store.TokenBucketAsync(keyA, capacity, refillRatePerSecond: 0.0001);
        var denied = await Store.TokenBucketAsync(keyA, capacity, refillRatePerSecond: 0.0001);
        Assert.False(denied.Allowed);

        var r = await Store.TokenBucketAsync(keyB, capacity, refillRatePerSecond: 0.0001);
        Assert.True(r.Allowed, "keyB should be unaffected by keyA being drained");
    }

    /// <summary>
    /// Drains the bucket then waits 1 second for a fast refill rate, proving tokens
    /// are actually replenished over time.
    /// </summary>
    [SkippableFact]
    public async Task TokenBucket_RefillsTokensOverTime()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int capacity = 2;
        const double refillRate = 4.0; // 4 tokens/s — 1 token every 250 ms

        // Drain completely.
        for (var i = 0; i < capacity; i++)
            await Store.TokenBucketAsync(key, capacity, refillRate);

        var denied = await Store.TokenBucketAsync(key, capacity, refillRate);
        Assert.False(denied.Allowed);

        // Wait 1 second — at 4 tokens/s, at least 1 token should have been restored.
        await Task.Delay(TimeSpan.FromSeconds(1));

        var r = await Store.TokenBucketAsync(key, capacity, refillRate);
        Assert.True(r.Allowed, "Token bucket should be allowed after refill delay");
    }

    [SkippableFact]
    public async Task PurgeExpired_CompletesWithoutError()
    {
        SkipIfUnavailable();
        await Store.SlidingWindowAsync(Unique(), 10, TimeSpan.FromSeconds(1));
        await Store.PurgeExpiredAsync(TimeSpan.Zero);
    }

    /// <summary>
    /// Fills a sliding-window key to its limit (so the next request would be denied),
    /// purges all rows, then verifies the next request is allowed — proving that
    /// <see cref="IRateLimitStore.PurgeExpiredAsync"/> physically removes the rows.
    /// </summary>
    [SkippableFact]
    public async Task PurgeExpired_DeletesExpiredWindowRows()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 3;
        var window = TimeSpan.FromMinutes(1);

        for (var i = 0; i < limit; i++)
            await Store.SlidingWindowAsync(key, limit, window);

        var denied = await Store.SlidingWindowAsync(key, limit, window);
        Assert.False(denied.Allowed, "Should be denied before purge");

        // Small delay so all inserted rows have window_start strictly < now() at purge time.
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        // maxAge=0 → delete everything with window_start < now().
        await Store.PurgeExpiredAsync(TimeSpan.Zero);

        // After purge the table is empty; this is the first request in a clean slate.
        var r = await Store.SlidingWindowAsync(key, limit, window);
        Assert.True(r.Allowed, "Should be allowed once old rows have been purged");
        Assert.Equal(limit - 1, r.Remaining);
    }

    /// <summary>
    /// Inserts a row with a recent timestamp and verifies it survives a purge
    /// with a longer <paramref name="maxAge"/> — proving the age filter is respected.
    /// </summary>
    [SkippableFact]
    public async Task PurgeExpired_DoesNotDeleteRowsWithinMaxAge()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int limit = 3;
        var window = TimeSpan.FromMinutes(5);

        // Insert a row that is 10 seconds old.
        var recent = DateTimeOffset.UtcNow - TimeSpan.FromSeconds(10);
        await InsertRowDirectlyAsync(ConnectionString!, _tableName, key, recent);

        // Purge with maxAge = 1 minute — the 10-second-old row must survive.
        await Store.PurgeExpiredAsync(TimeSpan.FromMinutes(1));

        // The row is still there, so this request sees count = 2 (existing + new).
        var r = await Store.SlidingWindowAsync(key, limit, window);
        Assert.True(r.Allowed, "Recent rows must not be deleted by PurgeExpiredAsync");
        Assert.Equal(limit - 2, r.Remaining); // existing row + this request = 2 counted
    }

    /// <summary>
    /// Drains a token bucket, calls <see cref="IRateLimitStore.PurgeExpiredAsync"/>,
    /// and verifies the bucket is still drained — proving the sentinel row is NOT
    /// deleted by the purge.
    /// </summary>
    [SkippableFact]
    public async Task PurgeExpired_PreservesTokenBucketState()
    {
        SkipIfUnavailable();
        var key = Unique();
        const int capacity = 2;

        for (var i = 0; i < capacity; i++)
            await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 0.0001);

        var denied = await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 0.0001);
        Assert.False(denied.Allowed);

        await Store.PurgeExpiredAsync(TimeSpan.Zero);

        // Sentinel must survive — bucket should still be drained.
        var r = await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 0.0001);
        Assert.False(r.Allowed,
            "Token bucket state must not be reset by PurgeExpiredAsync");
    }
}
