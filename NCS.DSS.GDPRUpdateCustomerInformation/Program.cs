using Azure.Identity;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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

                services.AddSingleton(s =>
                {
                    var cosmosDbEndpoint = Environment.GetEnvironmentVariable("CosmosDBEndpoint");
                    if (string.IsNullOrEmpty(cosmosDbEndpoint))
                    {
                        throw new InvalidOperationException("CosmosDbEndpoint is not configured.");
                    }

                    var options = new CosmosClientOptions() { ConnectionMode = ConnectionMode.Gateway };
                    return new CosmosClient(cosmosDbEndpoint, new DefaultAzureCredential(), options);
                });

                services.Configure<LoggerFilterOptions>(options =>
                {
                    // The Application Insights SDK adds a default logging filter that instructs ILogger to capture only Warning and more severe logs. Application Insights requires an explicit override.
                    // Log levels can also be configured using appsettings.json. For more information, see https://learn.microsoft.com/en-us/azure/azure-monitor/app/worker-service#ilogger-logs
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