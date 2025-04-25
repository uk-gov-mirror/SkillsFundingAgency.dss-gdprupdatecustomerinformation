using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace NCS.DSS.DataUtility.Functions
{
    public class SetGdprOperatingHours
    {
        private readonly ILogger _logger;

        public SetGdprOperatingHours(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<SetGdprOperatingHours>();
        }

        [Function("SetGdprOperatingHours")]
        public void Run([TimerTrigger("*/5 * * * *")] TimerInfo myTimer) // every 5 minutes
        {
            TimeSpan currentTime = DateTime.Now.TimeOfDay;
            TimeSpan stopTime = new TimeSpan(16, 10, 0);
            TimeSpan startTime = new TimeSpan(16, 20, 0);

            _logger.LogInformation($"CURRENT TIME: {currentTime.ToString()}");

            if ((currentTime > stopTime) && (currentTime < startTime))
            {
                Environment.SetEnvironmentVariable("AzureWebJobs.DeleteCustomerData.Disabled", "true");
            } 
            else
            {
                Environment.SetEnvironmentVariable("AzureWebJobs.DeleteCustomerData.Disabled", "false");
            }
        }
    }
}
