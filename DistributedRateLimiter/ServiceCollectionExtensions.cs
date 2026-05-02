using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;
using DistributedRateLimiter.Core.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace DistributedRateLimiter;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDbRateLimiter(
        this IServiceCollection services,
        IRateLimitStore store,
        Action<DbRateLimiterOptions> configure)
    {
        services.AddSingleton(store);
        services.Configure(configure);
        services.AddSingleton<IDbRateLimiter>(sp =>
            new DbRateLimiter(
                sp.GetRequiredService<IRateLimitStore>(),
                sp.GetRequiredService<IOptions<DbRateLimiterOptions>>().Value));
        return services;
    }

    /// <summary>
    /// Registers a background service that periodically deletes expired rate limit rows.
    /// Call this once — it is shared across all policies and providers.
    /// </summary>
    public static IServiceCollection AddDbRateLimiterCleanup(
        this IServiceCollection services,
        Action<RateLimitCleanupOptions>? configure = null)
    {
        if (configure is not null)
            services.Configure(configure);
        else
            services.Configure<RateLimitCleanupOptions>(_ => { }); // register with defaults

        services.AddHostedService<RateLimitCleanupWorker>();
        return services;
    }

}