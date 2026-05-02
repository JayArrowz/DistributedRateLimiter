using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Xunit;

namespace DistributedRateLimiter.IntegrationTests.Worker;

public sealed class RateLimitCleanupWorkerTests
{
    private static IHostedService BuildWorker(IRateLimitStore store, Action<RateLimitCleanupOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(store);
        services.AddDbRateLimiterCleanup(configure);
        return services.BuildServiceProvider().GetServices<IHostedService>().Single();
    }

    /// <summary>
    /// Verifies the worker passes the configured <see cref="RateLimitCleanupOptions.MaxAge"/>
    /// directly to <see cref="IRateLimitStore.PurgeExpiredAsync"/>.
    /// </summary>
    [Fact]
    public async Task Cleanup_CallsPurgeWithConfiguredMaxAge()
    {
        var store = Substitute.For<IRateLimitStore>();
        var maxAge = TimeSpan.FromHours(3);
        var called = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);

        store.PurgeExpiredAsync(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                called.TrySetResult(callInfo.Arg<TimeSpan>());
                return Task.CompletedTask;
            });

        var worker = BuildWorker(store, opts =>
        {
            opts.StartupStagger = TimeSpan.Zero;
            opts.MaxAge = maxAge;
            opts.Interval = TimeSpan.FromHours(1); // long — ensures only one run before we stop
        });

        await worker.StartAsync(CancellationToken.None);
        var passedMaxAge = await called.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(CancellationToken.None);

        Assert.Equal(maxAge, passedMaxAge);
    }

    /// <summary>
    /// Verifies that an exception thrown by <see cref="IRateLimitStore.PurgeExpiredAsync"/>
    /// is swallowed by the worker — it must not crash, and must keep retrying on the next interval.
    /// </summary>
    [Fact]
    public async Task Cleanup_ExceptionDoesNotCrashWorker()
    {
        var store = Substitute.For<IRateLimitStore>();
        var callCount = 0;

        store.PurgeExpiredAsync(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Interlocked.Increment(ref callCount);
                return Task.FromException(new InvalidOperationException("simulated DB failure"));
            });

        var worker = BuildWorker(store, opts =>
        {
            opts.StartupStagger = TimeSpan.Zero;
            opts.Interval = TimeSpan.FromMilliseconds(50);
        });

        await worker.StartAsync(CancellationToken.None);
        await Task.Delay(300); // allow multiple intervals to elapse

        // StopAsync must not throw — the worker must have swallowed the exceptions
        var ex = await Record.ExceptionAsync(() => worker.StopAsync(CancellationToken.None));
        Assert.Null(ex);

        // Confirmed it was called more than once despite continuous exceptions
        Assert.True(callCount > 1, $"Expected multiple cleanup attempts but got {callCount}");
    }
}
