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
                
                services.AddSingleton<ICosmosDBService, CosmosDBService>();
                services.AddSingleton<IIdentifyAndAnonymiseDataService, IdentifyAndAnonymiseDataService>();
                services.AddSingleton<IGenericDataService, GenericDataService>();
                services.AddSingleton<ISqlDbService, SqlDbService>();
                services.AddSingleton(s => new CosmosClient(Environment.GetEnvironmentVariable("CosmosDBConnectionString")));
                
                /*services.AddSingleton(sp =>
                {
                    ServiceBusClient client = new ServiceBusClient(Environment.GetEnvironmentVariable("serviceBusConnectionString"), new ServiceBusClientOptions
                    {
                        TransportType = ServiceBusTransportType.AmqpWebSockets
                    });

                    return client;
                });*/

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