using System;

namespace DistributedRateLimiter.Core.Model;

public sealed record RateLimitResult(
    bool Allowed,
    int Remaining,
    int Limit,
    TimeSpan RetryAfter);
