using DistributedRateLimiter.Core.Model;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DistributedRateLimiter.Core.Interface;

public interface IRateLimitStore
{
    Task EnsureSchemaAsync(CancellationToken ct = default);

    Task<RateLimitResult> SlidingWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken ct = default);

    Task<RateLimitResult> TokenBucketAsync(
        string key,
        int capacity,
        double refillRatePerSecond,
        CancellationToken ct = default);

    Task<RateLimitResult> FixedWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken ct = default);

    Task PurgeExpiredAsync(TimeSpan maxAge, CancellationToken ct = default);
}
