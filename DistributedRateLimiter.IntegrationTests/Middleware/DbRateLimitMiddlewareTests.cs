using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;

namespace DistributedRateLimiter.IntegrationTests.Middleware;

/// <summary>
/// Tests for <see cref="ApplicationBuilderExtensions.UseDbRateLimiter"/> using a fake
/// in-memory store — no real database required.
/// </summary>
public sealed class DbRateLimitMiddlewareTests
{
    private const string PolicyName = "test-policy";

    /// <summary>Builds a <see cref="TestServer"/> backed by the supplied store.</summary>
    private static TestServer BuildServer(IRateLimitStore store, int limit, TimeSpan window)
    {
        var builder = new WebHostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbRateLimiter(store, opts =>
                    opts.AddPolicy(new RateLimitPolicy
                    {
                        Name = PolicyName,
                        Algorithm = RateLimitAlgorithm.SlidingWindow,
                        Limit = limit,
                        Window = window
                    }));
            })
            .Configure(app =>
            {
                app.UseDbRateLimiter(ctx => ctx.Connection.RemoteIpAddress?.ToString() ?? "test", PolicyName);
                app.Run(ctx =>
                {
                    ctx.Response.StatusCode = 200;
                    return ctx.Response.WriteAsync("OK");
                });
            });

        return new TestServer(builder);
    }
    
    [Fact]
    public async Task Returns200_WhenRequestAllowed()
    {
        using var server = BuildServer(new FakeRateLimitStore(allowed: true, remaining: 9, limit: 10), 10, TimeSpan.FromMinutes(1));
        using var client = server.CreateClient();

        var response = await client.GetAsync("/");

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task SetsRateLimitHeaders_WhenRequestAllowed()
    {
        using var server = BuildServer(new FakeRateLimitStore(allowed: true, remaining: 7, limit: 10), 10, TimeSpan.FromMinutes(1));
        using var client = server.CreateClient();

        var response = await client.GetAsync("/");

        Assert.Equal("10", response.Headers.GetValues("X-RateLimit-Limit").Single());
        Assert.Equal("7", response.Headers.GetValues("X-RateLimit-Remaining").Single());
        Assert.False(response.Headers.Contains("Retry-After"));
    }

    [Fact]
    public async Task CallsNextMiddleware_WhenRequestAllowed()
    {
        using var server = BuildServer(new FakeRateLimitStore(allowed: true, remaining: 5, limit: 10), 10, TimeSpan.FromMinutes(1));
        using var client = server.CreateClient();

        var response = await client.GetAsync("/");
        var body = await response.Content.ReadAsStringAsync();

        Assert.Equal("OK", body);
    }

    [Fact]
    public async Task Returns429_WhenRequestDenied()
    {
        using var server = BuildServer(new FakeRateLimitStore(allowed: false, remaining: 0, limit: 3, retryAfter: TimeSpan.FromSeconds(30)), 3, TimeSpan.FromMinutes(1));
        using var client = server.CreateClient();

        var response = await client.GetAsync("/");

        Assert.Equal(System.Net.HttpStatusCode.TooManyRequests, response.StatusCode);
    }

    [Fact]
    public async Task SetsRetryAfterHeader_WhenRequestDenied()
    {
        var retryAfter = TimeSpan.FromSeconds(42);
        using var server = BuildServer(new FakeRateLimitStore(allowed: false, remaining: 0, limit: 3, retryAfter: retryAfter), 3, TimeSpan.FromMinutes(1));
        using var client = server.CreateClient();

        var response = await client.GetAsync("/");

        Assert.Equal("42", response.Headers.GetValues("Retry-After").Single());
    }

    [Fact]
    public async Task SetsRetryAfterHeader_CeilsFractionalSeconds()
    {
        // 42.4 s must be reported as 43 — rounding down would let clients retry too early.
        var retryAfter = TimeSpan.FromSeconds(42.4);
        using var server = BuildServer(new FakeRateLimitStore(allowed: false, remaining: 0, limit: 3, retryAfter: retryAfter), 3, TimeSpan.FromMinutes(1));
        using var client = server.CreateClient();

        var response = await client.GetAsync("/");

        Assert.Equal("43", response.Headers.GetValues("Retry-After").Single());
    }

    [Fact]
    public async Task SetsRateLimitHeaders_WhenRequestDenied()
    {
        using var server = BuildServer(new FakeRateLimitStore(allowed: false, remaining: 0, limit: 3, retryAfter: TimeSpan.FromSeconds(10)), 3, TimeSpan.FromMinutes(1));
        using var client = server.CreateClient();

        var response = await client.GetAsync("/");

        Assert.Equal("3", response.Headers.GetValues("X-RateLimit-Limit").Single());
        Assert.Equal("0", response.Headers.GetValues("X-RateLimit-Remaining").Single());
    }

    [Fact]
    public async Task DoesNotCallNextMiddleware_WhenRequestDenied()
    {
        using var server = BuildServer(new FakeRateLimitStore(allowed: false, remaining: 0, limit: 3, retryAfter: TimeSpan.FromSeconds(10)), 3, TimeSpan.FromMinutes(1));
        using var client = server.CreateClient();

        var response = await client.GetAsync("/");
        var body = await response.Content.ReadAsStringAsync();

        // Body should be the rate-limit message, NOT the downstream "OK"
        Assert.NotEqual("OK", body);
        Assert.False(string.IsNullOrWhiteSpace(body));
    }

    [Fact]
    public async Task DynamicPolicySelector_RoutesRequestToCorrectPolicy()
    {
        // Arrange two policies with different limits, chosen by a request header.
        const string premiumPolicy = "premium";
        const string standardPolicy = "standard";
        
        var store = Substitute.For<IRateLimitStore>();
        store.FixedWindowAsync(Arg.Any<string>(), 10, Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
             .Returns(new RateLimitResult(true, 9, 10, TimeSpan.Zero));
        store.FixedWindowAsync(Arg.Any<string>(), 100, Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
             .Returns(new RateLimitResult(true, 99, 100, TimeSpan.Zero));

        var builder = new WebHostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbRateLimiter(store, opts =>
                {
                    opts.AddPolicy(new RateLimitPolicy { Name = premiumPolicy, Algorithm = RateLimitAlgorithm.FixedWindow, Limit = 100, Window = TimeSpan.FromMinutes(1) });
                    opts.AddPolicy(new RateLimitPolicy { Name = standardPolicy, Algorithm = RateLimitAlgorithm.FixedWindow, Limit = 10, Window = TimeSpan.FromMinutes(1) });
                });
            })
            .Configure(app =>
            {
                app.UseDbRateLimiter(
                    keySelector: ctx => ctx.Connection.RemoteIpAddress?.ToString() ?? "test",
                    policyNameSelector: ctx => ctx.Request.Headers.ContainsKey("X-Premium") ? premiumPolicy : standardPolicy);
                app.Run(ctx => ctx.Response.WriteAsync("OK"));
            });

        using var server = new TestServer(builder);
        using var client = server.CreateClient();

        // Without the header → standard policy (limit=10)
        var standardResponse = await client.GetAsync("/");
        Assert.Equal("10", standardResponse.Headers.GetValues("X-RateLimit-Limit").Single());

        // With the header → premium policy (limit=100)
        var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Get, "/");
        request.Headers.Add("X-Premium", "1");
        var premiumResponse = await client.SendAsync(request);
        Assert.Equal("100", premiumResponse.Headers.GetValues("X-RateLimit-Limit").Single());
    }

    [Fact]
    public async Task CheckAsync_WithPolicyName_AllowsRequest_WhenWithinLimit()
    {
        var store = new FakeRateLimitStore(allowed: true, remaining: 4, limit: 5);
        var limiter = BuildLimiter(store, limit: 5);

        var result = await limiter.CheckAsync("user:1", PolicyName);

        Assert.True(result.Allowed);
        Assert.Equal(4, result.Remaining);
        Assert.Equal(5, result.Limit);
    }

    [Fact]
    public async Task CheckAsync_WithPolicyName_DeniesRequest_WhenOverLimit()
    {
        var store = new FakeRateLimitStore(allowed: false, remaining: 0, limit: 5, retryAfter: TimeSpan.FromSeconds(60));
        var limiter = BuildLimiter(store, limit: 5);

        var result = await limiter.CheckAsync("user:1", PolicyName);

        Assert.False(result.Allowed);
        Assert.Equal(0, result.Remaining);
        Assert.True(result.RetryAfter > TimeSpan.Zero);
    }

    [Fact]
    public async Task CheckAsync_WithUnknownPolicyName_ThrowsInvalidOperationException()
    {
        var store = new FakeRateLimitStore(allowed: true, remaining: 5, limit: 5);
        var limiter = BuildLimiter(store, limit: 5);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => limiter.CheckAsync("user:1", "does-not-exist"));
    }

    private static IDbRateLimiter BuildLimiter(IRateLimitStore store, int limit)
    {
        var services = new ServiceCollection();
        services.AddDbRateLimiter(store, opts =>
            opts.AddPolicy(new RateLimitPolicy
            {
                Name      = PolicyName,
                Algorithm = RateLimitAlgorithm.SlidingWindow,
                Limit     = limit,
                Window    = TimeSpan.FromMinutes(1)
            }));
        return services.BuildServiceProvider().GetRequiredService<IDbRateLimiter>();
    }
}

file sealed class FakeRateLimitStore : IRateLimitStore
{
    private readonly RateLimitResult _result;

    public FakeRateLimitStore(bool allowed, int remaining, int limit, TimeSpan retryAfter = default)
    {
        _result = new RateLimitResult(allowed, remaining, limit, retryAfter);
    }

    public Task EnsureSchemaAsync(CancellationToken ct = default) => Task.CompletedTask;

    public Task<RateLimitResult> SlidingWindowAsync(string key, int limit, TimeSpan window, CancellationToken ct = default)
        => Task.FromResult(_result);

    public Task<RateLimitResult> FixedWindowAsync(string key, int limit, TimeSpan window, CancellationToken ct = default)
        => Task.FromResult(_result);

    public Task<RateLimitResult> TokenBucketAsync(string key, int capacity, double refillRatePerSecond, CancellationToken ct = default)
        => Task.FromResult(_result);

    public Task PurgeExpiredAsync(TimeSpan maxAge, CancellationToken ct = default) => Task.CompletedTask;
}
