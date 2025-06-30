using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Services;
using System.Threading.Tasks;

namespace NCS.DSS.DataUtility
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var host = new HostBuilder().ConfigureFunctionsWebApplication().ConfigureServices(services =>
            {
                services.AddApplicationInsightsTelemetryWorkerService();
                services.ConfigureFunctionsApplicationInsights();

                services.AddSingleton<ISqlDbService, SqlDbService>();
                services.AddSingleton<IServiceBusService, ServiceBusService>();
                services.AddSingleton<ICosmosDatabaseService, CosmosDatabaseService>();

                services.AddSingleton(sp =>
                {
                    var logger = sp.GetRequiredService<ILogger<Program>>();


                    var connectionString = Environment.GetEnvironmentVariable("AdviserDetailConnectionString");
                    var endpoint = Environment.GetEnvironmentVariable("CosmosDbEndpoint");

                    var options = new CosmosClientOptions
                    {
                        ConnectionMode = ConnectionMode.Gateway
                    };

                    if (!string.IsNullOrWhiteSpace(endpoint))
                    {
                        logger.LogInformation("Using DefaultAzureCredential for Cosmos DB (managed identity)");
                        return new CosmosClient(endpoint, new DefaultAzureCredential(), options);
                    }
                    else if (!string.IsNullOrWhiteSpace(connectionString))
                    {
                        logger.LogInformation("No managed identity found: using Cosmos DB connection string (local development)");
                        return new CosmosClient(connectionString, options);
                    }
                    else
                    {
                        throw new InvalidOperationException("Neither CosmosDbEndpoint or a ConnectionString are configured");
                    }
                });

                services.AddSingleton(sp => new ServiceBusClient(Environment.GetEnvironmentVariable("ServiceBusConnectionString"), new ServiceBusClientOptions
                {
                    TransportType = ServiceBusTransportType.AmqpWebSockets
                }));

                services.Configure<LoggerFilterOptions>(options =>
                {
                    LoggerFilterRule toRemove = options.Rules.FirstOrDefault(rule => rule.ProviderName
                        == "Microsoft.Extensions.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider");

                    if (toRemove is not null)
                    {
                        options.Rules.Remove(toRemove);
                    }
                });
            }).Build();

            await host.RunAsync();
        }
    }
}