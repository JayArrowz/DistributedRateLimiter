using DistributedRateLimiter.Providers.MySql;
using DistributedRateLimiter.Core.Interface;
using Xunit;

namespace DistributedRateLimiter.IntegrationTests.Stores;

[Trait("Provider", "MySql")]
public sealed class MySqlRateLimitStoreTests : RateLimitStoreTests
{
    protected override string? ConnectionString =>
        Environment.GetEnvironmentVariable("MYSQL_CONNECTION_STRING");

    protected override string ProviderName => "MySQL";

    protected override IRateLimitStore CreateStore(string connectionString, string tableName) =>
        new MySqlRateLimitStore(connectionString, tableName);
}
