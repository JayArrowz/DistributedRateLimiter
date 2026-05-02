namespace DistributedRateLimiter.Core.Model;

public enum RateLimitAlgorithm
{
    SlidingWindow,
    FixedWindow,
    TokenBucket
}