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

    protected IRateLimitStore Store { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        if (ConnectionString is null) return;
        Store = CreateStore(ConnectionString, _tableName);
        await Store.EnsureSchemaAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private void SkipIfUnavailable() =>
        Skip.If(ConnectionString is null,
            $"{ProviderName} connection string not set. " +
            "Provide it via the appropriate environment variable.");

    [SkippableFact]
    public async Task SlidingWindow_AllowsRequestsWithinLimit()
    {
        SkipIfUnavailable();

        var key = $"sw_allow_{Guid.NewGuid():N}";
        const int limit = 5;

        for (var i = 1; i <= limit; i++)
        {
            var result = await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));
            Assert.True(result.Allowed, $"Request {i} should be allowed");
            Assert.Equal(limit, result.Limit);
        }
    }

    [SkippableFact]
    public async Task SlidingWindow_DeniesRequestOverLimit()
    {
        SkipIfUnavailable();

        var key = $"sw_deny_{Guid.NewGuid():N}";
        const int limit = 3;

        for (var i = 0; i < limit; i++)
            await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));

        var result = await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.False(result.Allowed);
        Assert.Equal(0, result.Remaining);
        Assert.True(result.RetryAfter > TimeSpan.Zero);
    }

    [SkippableFact]
    public async Task SlidingWindow_RemainingDecrementsCorrectly()
    {
        SkipIfUnavailable();

        var key = $"sw_rem_{Guid.NewGuid():N}";
        const int limit = 5;

        var first = await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.Equal(limit - 1, first.Remaining);

        var second = await Store.SlidingWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.Equal(limit - 2, second.Remaining);
    }
    
    [SkippableFact]
    public async Task FixedWindow_AllowsRequestsWithinLimit()
    {
        SkipIfUnavailable();

        var key = $"fw_allow_{Guid.NewGuid():N}";
        const int limit = 5;

        for (var i = 1; i <= limit; i++)
        {
            var result = await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));
            Assert.True(result.Allowed, $"Request {i} should be allowed");
            Assert.Equal(limit, result.Limit);
        }
    }

    [SkippableFact]
    public async Task FixedWindow_DeniesRequestOverLimit()
    {
        SkipIfUnavailable();

        var key = $"fw_deny_{Guid.NewGuid():N}";
        const int limit = 3;

        for (var i = 0; i < limit; i++)
            await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));

        var result = await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.False(result.Allowed);
        Assert.Equal(0, result.Remaining);
        Assert.True(result.RetryAfter > TimeSpan.Zero);
    }

    [SkippableFact]
    public async Task FixedWindow_RemainingDecrementsCorrectly()
    {
        SkipIfUnavailable();

        var key = $"fw_rem_{Guid.NewGuid():N}";
        const int limit = 5;

        var first = await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.Equal(limit - 1, first.Remaining);

        var second = await Store.FixedWindowAsync(key, limit, TimeSpan.FromMinutes(1));
        Assert.Equal(limit - 2, second.Remaining);
    }

    [SkippableFact]
    public async Task TokenBucket_AllowsRequestsWithinCapacity()
    {
        SkipIfUnavailable();

        var key = $"tb_allow_{Guid.NewGuid():N}";
        const int capacity = 5;

        for (var i = 1; i <= capacity; i++)
        {
            var result = await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 1);
            Assert.True(result.Allowed, $"Request {i} should be allowed");
        }
    }

    [SkippableFact]
    public async Task TokenBucket_DeniesRequestOverCapacity()
    {
        SkipIfUnavailable();

        var key = $"tb_deny_{Guid.NewGuid():N}";
        const int capacity = 3;

        // Use an extremely slow refill rate so no tokens are restored between calls.
        for (var i = 0; i < capacity; i++)
            await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 0.0001);

        var result = await Store.TokenBucketAsync(key, capacity, refillRatePerSecond: 0.0001);
        Assert.False(result.Allowed);
        Assert.Equal(0, result.Remaining);
    }
    
    [SkippableFact]
    public async Task PurgeExpired_CompletesWithoutError()
    {
        SkipIfUnavailable();
        // Insert a record then immediately purge so there is at least one row to act on.
        await Store.SlidingWindowAsync($"purge_{Guid.NewGuid():N}", 10, TimeSpan.FromSeconds(1));
        await Store.PurgeExpiredAsync(TimeSpan.Zero);
    }
}
