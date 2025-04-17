using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Services;

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
                    var options = new CosmosClientOptions()
                    {
                        ConnectionMode = ConnectionMode.Gateway
                    };

                    return new CosmosClient(Environment.GetEnvironmentVariable("CosmosDBConnectionString"), options);
                });

                services.AddSingleton(sp =>
                {
                    ServiceBusClient client = new ServiceBusClient(Environment.GetEnvironmentVariable("ServiceBusConnectionString"), new ServiceBusClientOptions
                    {
                        TransportType = ServiceBusTransportType.AmqpWebSockets
                    });

                    return client;
                });

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