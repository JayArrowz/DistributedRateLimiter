using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace DistributedRateLimiter;

public static class ApplicationBuilderExtensions
{
    public static IApplicationBuilder UseDbRateLimiter(
        this IApplicationBuilder app,
        Func<HttpContext, string> keySelector,
        string policyName)
    {
        return app.Use(next => async context =>
        {
            var limiter = context.RequestServices.GetRequiredService<IDbRateLimiter>();
            var options = context.RequestServices
                .GetRequiredService<IOptions<DbRateLimiterOptions>>();

            var key = keySelector(context);
            var policy = options.Value.GetPolicy(policyName);
            var result = await limiter.CheckAsync(key, policy, context.RequestAborted);

            context.Response.Headers["X-RateLimit-Limit"] = result.Limit.ToString();
            context.Response.Headers["X-RateLimit-Remaining"] = result.Remaining.ToString();

            if (!result.Allowed)
            {
                context.Response.Headers["Retry-After"] =
                    result.RetryAfter.TotalSeconds.ToString("0");
                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                await context.Response.WriteAsync("Rate limit exceeded.", context.RequestAborted);
                return;
            }

            await next(context);
        });
    }
}