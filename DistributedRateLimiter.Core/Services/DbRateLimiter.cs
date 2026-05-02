using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DistributedRateLimiter.Core.Services;

public sealed class DbRateLimiter : IDbRateLimiter
{
    private readonly IRateLimitStore _store;
    private readonly DbRateLimiterOptions _options;

    public DbRateLimiter(IRateLimitStore store, DbRateLimiterOptions options)
    {
        _store = store;
        _options = options;
    }

    public Task<RateLimitResult> CheckAsync(
        string key,
        string policyName,
        CancellationToken ct = default)
        => CheckAsync(key, _options.GetPolicy(policyName), ct);

    public Task<RateLimitResult> CheckAsync(
        string key,
        RateLimitPolicy policy,
        CancellationToken ct = default)
    {
        return policy.Algorithm switch
        {
            RateLimitAlgorithm.SlidingWindow =>
                _store.SlidingWindowAsync(key, policy.Limit, policy.Window, ct),

            RateLimitAlgorithm.FixedWindow =>
                _store.FixedWindowAsync(key, policy.Limit, policy.Window, ct),

            RateLimitAlgorithm.TokenBucket =>
                _store.TokenBucketAsync(
                    key,
                    policy.BucketCapacity
                        ?? throw new InvalidOperationException(
                            "BucketCapacity must be set for TokenBucket policy."),
                    policy.RefillRatePerSecond
                        ?? throw new InvalidOperationException(
                            "RefillRatePerSecond must be set for TokenBucket policy."),
                    ct),

            _ => throw new NotSupportedException(
                $"Algorithm {policy.Algorithm} is not supported.")
        };
    }
}