using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DistributedRateLimiter;

internal sealed class RateLimitCleanupWorker : BackgroundService
{
    private readonly IRateLimitStore _store;
    private readonly RateLimitCleanupOptions _options;
    private readonly ILogger<RateLimitCleanupWorker> _logger;

    public RateLimitCleanupWorker(
        IRateLimitStore store,
        IOptions<RateLimitCleanupOptions> options,
        ILogger<RateLimitCleanupWorker> logger)
    {
        _store = store;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await Task.Delay(_options.StartupStagger, ct);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                _logger.LogDebug(
                    "DbRateLimiter: purging rows older than {MaxAge}", _options.MaxAge);

                await _store.PurgeExpiredAsync(_options.MaxAge, ct);

                _logger.LogDebug(
                    "DbRateLimiter: cleanup complete, next run in {Interval}", _options.Interval);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Shutting down — exit cleanly
                break;
            }
            catch (Exception ex)
            {
                // Don't let a cleanup failure crash the worker — just log and retry next interval
                _logger.LogError(ex, "DbRateLimiter: cleanup failed, will retry in {Interval}",
                    _options.Interval);
            }

            await Task.Delay(_options.Interval, ct);
        }
    }
}