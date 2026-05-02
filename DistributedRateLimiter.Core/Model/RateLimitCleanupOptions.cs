using System;

namespace DistributedRateLimiter.Core.Model;

public sealed class RateLimitCleanupOptions
{
    /// <summary>
    /// How long to wait after application startup before the cleanup worker starts running.
    /// </summary>
    public TimeSpan StartupStagger { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How old a rate limit row must be before it is eligible for deletion.
    /// Defaults to 2 hours — set this to at least your longest window duration.
    /// </summary>
    public TimeSpan MaxAge { get; set; } = TimeSpan.FromHours(2);

    /// <summary>
    /// How often the cleanup worker runs.
    /// Defaults to 30 minutes.
    /// </summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromMinutes(30);
}
