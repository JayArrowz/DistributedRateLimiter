using System;

namespace DistributedRateLimiter.Core.Extensions;

public static class TimespanExtensions
{
    public static DateTimeOffset GetFixedWindowStart(this TimeSpan window)
    {
        var now = DateTimeOffset.UtcNow;
        return window.TotalSeconds switch
        {
            60 => new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, TimeSpan.Zero),
            3600 => new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, 0, 0, TimeSpan.Zero),
            86400 => new DateTimeOffset(now.Year, now.Month, now.Day, 0, 0, 0, TimeSpan.Zero),
            _ => throw new ArgumentOutOfRangeException(nameof(window),
                "FixedWindow on SQL Server supports only 1-minute, 1-hour, and 1-day windows. " +
                "Use SlidingWindow for arbitrary durations.")
        };
    }
    public static string ToPostgresTruncUnit(this TimeSpan window) => window.TotalSeconds switch
    {
        60 => "minute",
        3600 => "hour",
        86400 => "day",
        _ => throw new ArgumentOutOfRangeException(nameof(window),
            "FixedWindow on Postgres supports only 1-minute, 1-hour, and 1-day windows. " +
            "Use SlidingWindow for arbitrary durations.")
    };
}
