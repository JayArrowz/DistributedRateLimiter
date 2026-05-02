# DistributedRateLimiter

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![NuGet](https://img.shields.io/nuget/v/DistributedRateLimiter.svg)](https://www.nuget.org/packages/DistributedRateLimiter)

Distributed rate limiting for .NET backed by your existing database. No Redis, no message broker, no extra infrastructure.

Works in HTTP pipelines **and** background services — any code that calls an external API or shared resource can enforce a rate limit that's consistent across every running instance.

---

## How it works

Each rate limit check is a single atomic SQL upsert. The counter lives in a table in your existing database, so all running instances share the same state automatically — no in-memory counters that diverge across pods.

```
Instance A                          Instance B
│                                   │
│  POST /orders                     │  POST /orders
│  → CheckAsync("user:42", policy)  │  → CheckAsync("user:42", policy)
│  → INSERT ... ON CONFLICT         │  → INSERT ... ON CONFLICT
│    DO UPDATE SET count + 1        │    DO UPDATE SET count + 1
│  ← count = 1  ✓ allowed           │  ← count = 2  ✓ allowed
│                                   │
│  POST /orders (101st request)     │
│  → CheckAsync("user:42", policy)  │
│  ← count = 101  ✗ 429             │
```

The database row is the single source of truth. No synchronisation needed between instances.

---

## Algorithms

| Algorithm | Best for |
|---|---|
| **SlidingWindow** | Smooth enforcement — spreads requests evenly across time |
| **FixedWindow** | Simpler and cheaper — slightly bursty at window boundaries |
| **TokenBucket** | Burst-tolerant — allows short spikes up to a capacity, then enforces a steady refill rate |

---

## Projects

| Project | Target | Description |
|---|---|---|
| `DistributedRateLimiter.Core` | `netstandard2.0` | Interfaces, models, and service contracts — no dependencies |
| `DistributedRateLimiter` | `net8.0;net9.0;net10.0` | Middleware, cleanup worker, and DI registration |
| `DistributedRateLimiter.Providers.Postgres` | `net8.0;net9.0;net10.0` | PostgreSQL provider via Npgsql |
| `DistributedRateLimiter.Providers.MsSql` | `net8.0;net9.0;net10.0` | SQL Server provider via Microsoft.Data.SqlClient |
| `DistributedRateLimiter.Providers.MySql` | `net8.0;net9.0;net10.0` | MySQL/MariaDB provider via MySqlConnector |

---

## Quick start

### 1. Install packages

```bash
dotnet add package DistributedRateLimiter
dotnet add package DistributedRateLimiter.Providers.Postgres
```

### 2. Register with the host

```csharp
using DistributedRateLimiter;
using DistributedRateLimiter.Core.Model;
using DistributedRateLimiter.Providers.Postgres;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbRateLimiter(
    new PostgresRateLimitStore(connectionString, tableName: "__rate_limits"),
    opts => opts.AddPolicy(new RateLimitPolicy
    {
        Name      = "api",
        Algorithm = RateLimitAlgorithm.SlidingWindow,
        Limit     = 100,
        Window    = TimeSpan.FromMinutes(1)
    }));

var app = builder.Build();
```

### 3. Add the middleware

```csharp
// Limit by authenticated user, falling back to IP
app.UseDbRateLimiter(
    keySelector: ctx => ctx.User.Identity?.Name
                     ?? ctx.Connection.RemoteIpAddress?.ToString()
                     ?? "anon",
    policyName: "api");

app.MapControllers();
app.Run();
```

That's it. Every request now increments a shared counter in your database. When a key exceeds the limit the middleware returns `429 Too Many Requests` with standard rate limit headers.

---

## Response headers

| Header | Description |
|---|---|
| `X-RateLimit-Limit` | Maximum requests allowed in the window |
| `X-RateLimit-Remaining` | Requests remaining in the current window |
| `Retry-After` | Seconds until the client may retry (only present on 429 responses) |

---

## Multiple policies

Register as many policies as you need — each is identified by name:

```csharp
builder.Services.AddDbRateLimiter(
    new PostgresRateLimitStore(connectionString, "__rate_limits"),
    opts =>
    {
        opts.AddPolicy(new RateLimitPolicy
        {
            Name      = "authenticated",
            Algorithm = RateLimitAlgorithm.SlidingWindow,
            Limit     = 1000,
            Window    = TimeSpan.FromMinutes(1)
        });

        opts.AddPolicy(new RateLimitPolicy
        {
            Name      = "anonymous",
            Algorithm = RateLimitAlgorithm.SlidingWindow,
            Limit     = 20,
            Window    = TimeSpan.FromMinutes(1)
        });

        opts.AddPolicy(new RateLimitPolicy
        {
            Name                = "webhooks",
            Algorithm           = RateLimitAlgorithm.TokenBucket,
            BucketCapacity      = 500,
            RefillRatePerSecond = 50
        });
    });

app.UseDbRateLimiter(
    keySelector: ctx => ctx.User.Identity?.Name
                     ?? ctx.Connection.RemoteIpAddress?.ToString()
                     ?? "anon",
    policyName: ctx => ctx.User.Identity?.IsAuthenticated == true
        ? "authenticated"
        : "anonymous");
```

---

## Using in background services

`IDbRateLimiter` lives in `DistributedRateLimiter.Core` and has no HTTP dependency — inject it anywhere.

Register the policy at startup alongside your other policies:

```csharp
builder.Services.AddDbRateLimiter(
    new PostgresRateLimitStore(connectionString, "__rate_limits"),
    opts =>
    {
        opts.AddPolicy(new RateLimitPolicy { Name = "api", /* ... */ });

        opts.AddPolicy(new RateLimitPolicy
        {
            Name                = "payment-gateway",
            Algorithm           = RateLimitAlgorithm.TokenBucket,
            BucketCapacity      = 100,
            RefillRatePerSecond = 100
        });
    });
```

Then inject `IDbRateLimiter` and call it by name:

```csharp
using DistributedRateLimiter.Core.Interface;

public class OrderProcessingWorker : BackgroundService
{
    private readonly IDbRateLimiter _limiter;
    private readonly IOrderRepository _orders;
    private readonly IPaymentGateway _payments;

    public OrderProcessingWorker(
        IDbRateLimiter limiter,
        IOrderRepository orders,
        IPaymentGateway payments)
    {
        _limiter = limiter;
        _orders = orders;
        _payments = payments;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var orders = await _orders.GetPendingAsync(ct);

            foreach (var order in orders)
            {
                // Enforce the payment gateway's per-merchant API quota
                // consistently across all running instances of this worker
                var result = await _limiter.CheckAsync(
                    key:        $"payment-gateway:{order.MerchantId}",
                    policyName: "payment-gateway",
                    ct);

                if (!result.Allowed)
                {
                    await Task.Delay(result.RetryAfter, ct);
                    continue;
                }

                await _payments.ChargeAsync(order, ct);
            }

            await Task.Delay(TimeSpan.FromSeconds(5), ct);
        }
    }
}
```

This is where a database-backed limiter has a meaningful advantage over in-memory alternatives — if you run 5 instances of this worker, an in-memory limiter allows 5× the intended quota. The shared database counter enforces the real limit regardless of instance count.

---

## Cleanup

Rate limit rows accumulate over time. Register the built-in cleanup worker to purge them on a schedule:

```csharp
// Default: purge rows older than 2 hours, run every 30 minutes
builder.Services.AddDbRateLimiterCleanup();

// Custom intervals
builder.Services.AddDbRateLimiterCleanup(opts =>
{
    opts.MaxAge   = TimeSpan.FromHours(1);   // set to at least your longest window
    opts.Interval = TimeSpan.FromMinutes(10);
});
```

`RateLimitCleanupOptions` lives in `DistributedRateLimiter.Core.Model`:

| Property | Default | Description |
|---|---|---|
| `MaxAge` | `2 hours` | Rows older than this are deleted. Set to at least your longest window duration |
| `Interval` | `30 minutes` | How often the cleanup worker runs |

The worker starts with a 30-second delay on boot so it doesn't fire immediately on every instance restart, and any exception during cleanup is caught and logged — it retries on the next interval rather than crashing.

---

## Algorithms in depth

### SlidingWindow

Divides time into buckets aligned to the window duration. Each request increments the counter for the current bucket. Provides smooth, consistent enforcement with no burst at window boundaries.

```csharp
new RateLimitPolicy
{
    Name      = "api",
    Algorithm = RateLimitAlgorithm.SlidingWindow,
    Limit     = 100,
    Window    = TimeSpan.FromMinutes(1)
}
```

### FixedWindow

Simpler and cheaper than sliding window — one row per key, reset at each window boundary. Allows a burst of up to `2 × Limit` at a boundary (the last requests of one window plus the first of the next).

```csharp
new RateLimitPolicy
{
    Name      = "api",
    Algorithm = RateLimitAlgorithm.FixedWindow,
    Limit     = 100,
    Window    = TimeSpan.FromMinutes(1)
}
```

> FixedWindow supports 1-minute, 1-hour, and 1-day windows on all providers. Use SlidingWindow for arbitrary durations.

### TokenBucket

Maintains a token count per key. Each request consumes one token. Tokens refill continuously at `RefillRatePerSecond` up to `BucketCapacity`. Allows short bursts while enforcing a long-run average rate.

```csharp
new RateLimitPolicy
{
    Name                = "api",
    Algorithm           = RateLimitAlgorithm.TokenBucket,
    BucketCapacity      = 200,   // burst up to 200
    RefillRatePerSecond = 10     // steady-state: 10 req/s
}
```

---

## Database providers

### PostgreSQL

```csharp
using DistributedRateLimiter.Providers.Postgres;

new PostgresRateLimitStore(connectionString, tableName: "__rate_limits")
```

`EnsureSchemaAsync` creates the table if it does not exist:

```sql
CREATE TABLE IF NOT EXISTS __rate_limits (
    key          TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    count        INT         NOT NULL DEFAULT 0,
    tokens       FLOAT       NULL,
    last_refill  TIMESTAMPTZ NULL,
    PRIMARY KEY (key, window_start)
);
```

All three algorithms use a single atomic round-trip via `INSERT ... ON CONFLICT DO UPDATE ... RETURNING`.

**Minimum version:** PostgreSQL 9.5+ (required for `ON CONFLICT DO UPDATE`). PostgreSQL 12+ recommended.

**Package:** `Npgsql` 8.x+. Targets `net8.0`, `net9.0`, `net10.0`.

---

### SQL Server

```csharp
using DistributedRateLimiter.Providers.MsSql;

new MsSqlRateLimitStore(connectionString, tableName: "__rate_limits")
```

`EnsureSchemaAsync` creates the table if it does not exist:

```sql
IF OBJECT_ID(N'[__rate_limits]', N'U') IS NULL
CREATE TABLE [__rate_limits] (
    key          NVARCHAR(512)  NOT NULL,
    window_start DATETIMEOFFSET NOT NULL,
    count        INT            NOT NULL DEFAULT 0,
    tokens       FLOAT          NULL,
    last_refill  DATETIMEOFFSET NULL,
    PRIMARY KEY (key, window_start)
);
```

Uses `MERGE ... WITH (HOLDLOCK)` for atomic upserts. Requires SQL Server 2022+ for the `GREATEST` function used in the token bucket algorithm.

**Package:** `Microsoft.Data.SqlClient` 6.x. Targets `net8.0`, `net9.0`, `net10.0`.

---

### MySQL / MariaDB

```csharp
using DistributedRateLimiter.Providers.MySql;

new MySqlRateLimitStore(connectionString, tableName: "__rate_limits")
```

`EnsureSchemaAsync` creates the table if it does not exist:

```sql
CREATE TABLE IF NOT EXISTS `__rate_limits` (
    `key`          VARCHAR(512) NOT NULL,
    `window_start` DATETIME(6)  NOT NULL,
    `count`        INT          NOT NULL DEFAULT 0,
    `tokens`       DOUBLE       NULL,
    `last_refill`  DATETIME(6)  NULL,
    PRIMARY KEY (`key`, `window_start`)
);
```

> MySQL/MariaDB does not support `RETURNING` on upserts so each check costs 2 round-trips instead of 1. Each check is wrapped in a transaction with `SELECT ... FOR UPDATE` to guarantee atomicity — this requires InnoDB (the default engine since MySQL 5.5).

**Minimum version:** MySQL 5.7+ or MariaDB 10.2+. `DATETIME(6)` microsecond precision requires MySQL 5.6+ / MariaDB 5.3+.

**Package:** `MySqlConnector` 2.x. Targets `net8.0`, `net9.0`, `net10.0`.

---

## Custom provider

Implement `IRateLimitStore` from `DistributedRateLimiter.Core`:

```csharp
using DistributedRateLimiter.Core.Interface;
using DistributedRateLimiter.Core.Model;

public sealed class MyCustomStore : IRateLimitStore
{
    public Task EnsureSchemaAsync(CancellationToken ct = default) { ... }

    public Task<RateLimitResult> SlidingWindowAsync(
        string key, int limit, TimeSpan window, CancellationToken ct = default) { ... }

    public Task<RateLimitResult> FixedWindowAsync(
        string key, int limit, TimeSpan window, CancellationToken ct = default) { ... }

    public Task<RateLimitResult> TokenBucketAsync(
        string key, int capacity, double refillRatePerSecond, CancellationToken ct = default) { ... }

    public Task PurgeExpiredAsync(TimeSpan maxAge, CancellationToken ct = default) { ... }
}
```

Pass it directly to `AddDbRateLimiter`:

```csharp
builder.Services.AddDbRateLimiter(new MyCustomStore(), opts => { ... });
```

---

## Configuration reference

### RateLimitPolicy

| Property | Required | Description |
|---|---|---|
| `Name` | ✓ | Unique policy identifier used in `UseDbRateLimiter` and `CheckAsync` |
| `Algorithm` | ✓ | `SlidingWindow`, `FixedWindow`, or `TokenBucket` |
| `Limit` | Window algorithms | Maximum requests per window |
| `Window` | Window algorithms | Duration of the window |
| `BucketCapacity` | `TokenBucket` | Maximum tokens (burst ceiling) |
| `RefillRatePerSecond` | `TokenBucket` | Tokens added per second |

### RateLimitCleanupOptions

| Property | Default | Description |
|---|---|---|
| `MaxAge` | `2 hours` | Minimum age of a row before it is eligible for deletion |
| `Interval` | `30 minutes` | How often the cleanup worker runs |

---

## When to use this vs Redis

A database-backed rate limiter is a good fit when:

- Your stack does not already include Redis
- Traffic is moderate — up to a few hundred requests per second per key
- You need distributed enforcement across instances without adding infrastructure
- You are already on a supported database for other purposes

If you are already running Redis, or a single key receives thousands of requests per second, a Redis-backed limiter will offer lower latency and higher throughput.