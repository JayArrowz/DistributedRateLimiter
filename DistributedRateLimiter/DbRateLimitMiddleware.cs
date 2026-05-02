using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace DistributedRateLimiter;

internal sealed class DbRateLimitMiddleware
{
    private readonly RequestDelegate _next;
    private readonly IDbRateLimiter _limiter;
    private readonly Func<HttpContext, string> _keySelector;
    private readonly Func<HttpContext, RateLimitPolicy> _policySelector;

    public DbRateLimitMiddleware(
        RequestDelegate next,
        IDbRateLimiter limiter,
        IOptions<DbRateLimiterOptions> options,
        Func<HttpContext, string> keySelector,
        Func<HttpContext, string> policyNameSelector)
    {
        _next = next;
        _limiter = limiter;
        _keySelector = keySelector;
        var opts = options.Value;
        _policySelector = ctx => opts.GetPolicy(policyNameSelector(ctx));
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var key = _keySelector(context);
        var result = await _limiter.CheckAsync(key, _policySelector(context), context.RequestAborted);

        context.Response.Headers["X-RateLimit-Limit"] = result.Limit.ToString();
        context.Response.Headers["X-RateLimit-Remaining"] = result.Remaining.ToString();

        if (!result.Allowed)
        {
            context.Response.Headers["Retry-After"] =
                ((int)Math.Ceiling(result.RetryAfter.TotalSeconds)).ToString();
            context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
            await context.Response.WriteAsync(
                "Rate limit exceeded. Please retry later.", context.RequestAborted);
            return;
        }

        await _next(context);
    }
}