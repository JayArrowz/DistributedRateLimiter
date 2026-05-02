using DistributedRateLimiter.Core.Model;
using System.Threading;
using System.Threading.Tasks;

namespace DistributedRateLimiter.Core.Interface;

public interface IDbRateLimiter
{
    /// <summary>
    /// Checks the rate limit using an explicit policy.
    /// Useful for dynamic or ad-hoc policies that are not registered at startup.
    /// </summary>
    Task<RateLimitResult> CheckAsync(
        string key,
        RateLimitPolicy policy,
        CancellationToken ct = default);

    /// <summary>
    /// Checks the rate limit using a named policy registered via <c>AddDbRateLimiter</c>.
    /// </summary>
    Task<RateLimitResult> CheckAsync(
        string key,
        string policyName,
        CancellationToken ct = default);
}