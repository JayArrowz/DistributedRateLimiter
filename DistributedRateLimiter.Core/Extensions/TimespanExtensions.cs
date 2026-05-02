using System;

namespace DistributedRateLimiter.Core.Extensions;

public static class TimespanExtensions
{
    public static DateTimeOffset GetFixedWindowStart(this TimeSpan window)
    {
        var totalSeconds = (long)window.TotalSeconds;
        if (totalSeconds <= 0)
            throw new ArgumentOutOfRangeException(nameof(window),
                "FixedWindow window must be a positive whole number of seconds.");

        var unixNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        return DateTimeOffset.FromUnixTimeSeconds(unixNow / totalSeconds * totalSeconds);
    }
}
