using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace DistributedRateLimiter;

internal sealed class DbRateLimitMiddleware
{
    private readonly RequestDelegate _next;
    private readonly IDbRateLimiter _limiter;
    private readonly RateLimitPolicy _policy;
    private readonly Func<HttpContext, string> _keySelector;

    public DbRateLimitMiddleware(
        RequestDelegate next,
        IDbRateLimiter limiter,
        IOptions<DbRateLimiterOptions> options,
        Func<HttpContext, string> keySelector,
        string policyName)
    {
        _next = next;
        _limiter = limiter;
        _keySelector = keySelector;
        _policy = options.Value.GetPolicy(policyName);
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var key = _keySelector(context);
        var result = await _limiter.CheckAsync(key, _policy, context.RequestAborted);

        context.Response.Headers["X-RateLimit-Limit"] = result.Limit.ToString();
        context.Response.Headers["X-RateLimit-Remaining"] = result.Remaining.ToString();

        if (!result.Allowed)
        {
            context.Response.Headers["Retry-After"] =
                result.RetryAfter.TotalSeconds.ToString("0");
            context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
            await context.Response.WriteAsync(
                "Rate limit exceeded. Please retry later.", context.RequestAborted);
            return;
        }

        await _next(context);
    }
}