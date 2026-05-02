using System;
using System.Collections.Generic;

namespace DistributedRateLimiter.Core.Model;

public sealed class DbRateLimiterOptions
{
    private readonly Dictionary<string, RateLimitPolicy> _policies = new();

    public DbRateLimiterOptions AddPolicy(RateLimitPolicy policy)
    {
        _policies[policy.Name] = policy;
        return this;
    }

    public RateLimitPolicy GetPolicy(string name) =>
        _policies.TryGetValue(name, out var policy)
            ? policy
            : throw new InvalidOperationException(
                $"No rate limit policy named '{name}' has been registered.");
}