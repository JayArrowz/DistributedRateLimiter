using System;

namespace DistributedRateLimiter.Core.Model;

public sealed class RateLimitPolicy
{
    /// <summary>
    /// Gets the name associated with the current instance.
    /// </summary>
    public string Name { get; init; } = default!;

    /// <summary>
    /// Gets the rate limiting algorithm to use for processing requests.
    /// </summary>
    /// <remarks>The selected algorithm determines how rate limits are enforced. Common algorithms include
    /// fixed window and sliding window. Choose an algorithm that best fits the application's traffic patterns and rate
    /// limiting requirements.</remarks>
    public RateLimitAlgorithm Algorithm { get; init; } = RateLimitAlgorithm.SlidingWindow;

    /// <summary>
    /// Gets the maximum number of items to return in a single operation.
    /// </summary>
    /// <remarks>Set this property to limit the size of the result set. A value less than or equal to zero may
    /// indicate no limit, depending on the implementation.</remarks>
    public int Limit { get; init; }

    /// <summary>
    /// Gets the duration of the window used for rate limiting or aggregation operations.
    /// </summary>
    /// <remarks>The window determines the time interval over which requests or events are counted. Adjust
    /// this value to control how frequently limits reset or aggregates are calculated.</remarks>
    public TimeSpan Window { get; init; }

    /// <summary>
    /// Gets the maximum number of tokens that the bucket can hold.
    /// </summary>
    /// <remarks>A null value indicates that the bucket has no capacity limit. This property is typically used
    /// to control the burst size in token bucket rate limiting algorithms.</remarks>
    public int? BucketCapacity { get; init; }

    /// <summary>
    /// Gets the number of tokens added to the bucket per second.
    /// </summary>
    /// <remarks>A null value indicates that the refill rate is not set and the default rate will be used.
    /// This property is typically used to control the rate at which tokens are replenished in a token bucket algorithm
    /// for rate limiting scenarios.</remarks>
    public double? RefillRatePerSecond { get; init; } // token bucket
}
