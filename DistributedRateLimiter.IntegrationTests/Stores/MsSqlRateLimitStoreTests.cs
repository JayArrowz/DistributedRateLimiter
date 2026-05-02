using DistributedRateLimiter.Providers.MsSql;
using DistributedRateLimiter.Core.Interface;
using Xunit;

namespace DistributedRateLimiter.IntegrationTests.Stores;

[Trait("Provider", "MsSql")]
public sealed class MsSqlRateLimitStoreTests : RateLimitStoreTests
{
    protected override string? ConnectionString =>
        Environment.GetEnvironmentVariable("MSSQL_CONNECTION_STRING");

    protected override string ProviderName => "SQL Server";

    protected override IRateLimitStore CreateStore(string connectionString, string tableName) =>
        new MsSqlRateLimitStore(connectionString, tableName);
}
