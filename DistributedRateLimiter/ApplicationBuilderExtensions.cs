using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace DistributedRateLimiter;

public static class ApplicationBuilderExtensions
{
    public static IApplicationBuilder UseDbRateLimiter(
        this IApplicationBuilder app,
        Func<HttpContext, string> keySelector,
        string policyName)
    {
        return app.UseMiddleware<DbRateLimitMiddleware>(keySelector, (Func<HttpContext, string>)(_ => policyName));
    }

    public static IApplicationBuilder UseDbRateLimiter(
        this IApplicationBuilder app,
        Func<HttpContext, string> keySelector,
        Func<HttpContext, string> policyNameSelector)
    {
        return app.UseMiddleware<DbRateLimitMiddleware>(keySelector, policyNameSelector);
    }
}