using DistributedRateLimiter.Providers.Postgres;
using DistributedRateLimiter.Core.Interface;
using Xunit;

namespace DistributedRateLimiter.IntegrationTests.Stores;

[Trait("Provider", "Postgres")]
public sealed class PostgresRateLimitStoreTests : RateLimitStoreTests
{
    protected override string? ConnectionString =>
        Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");

    protected override string ProviderName => "PostgreSQL";

    protected override IRateLimitStore CreateStore(string connectionString, string tableName) =>
        new PostgresRateLimitStore(connectionString, tableName);
}
