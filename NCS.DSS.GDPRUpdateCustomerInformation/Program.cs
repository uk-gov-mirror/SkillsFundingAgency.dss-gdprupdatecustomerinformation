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
            var cosmosConnectionString = Environment.GetEnvironmentVariable("CosmosDBConnectionString");

            var host = new HostBuilder().ConfigureFunctionsWebApplication().ConfigureServices(services =>
            {
                services.AddApplicationInsightsTelemetryWorkerService();
                services.ConfigureFunctionsApplicationInsights();

                services.AddSingleton<ICosmosDBService, CosmosDBService>();
                services.AddSingleton<IIdentifyAndAnonymiseDataService, IdentifyAndAnonymiseDataService>();
                services.AddSingleton<IGenericDataService, GenericDataService>();

                services.AddSingleton(s => new CosmosClient(cosmosConnectionString));

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