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
            DateTime currentDateTimeUTC = DateTime.Now.ToUniversalTime();
            TimeSpan currentTime = DateTime.Now.TimeOfDay;

            
            
            //var dt = new DateTime(localTime.Ticks);
            //var utc = dt.ToUniversalTime();
            //return new TimeSpan(utc.Ticks);

            TimeSpan stopTime = new TimeSpan(16, 10, 0);
            TimeSpan startTime = new TimeSpan(16, 20, 0);
            TimeSpan timespanUTC = new TimeSpan(currentDateTimeUTC.Ticks);

            _logger.LogInformation($"CURRENT TIME: {currentTime.ToString()}");
            _logger.LogInformation($"CURRENT TIME UTC: {currentDateTimeUTC.ToString()}");

            _logger.LogInformation($"timespan UTC: {timespanUTC.ToString()}");
            _logger.LogInformation($"stop: {stopTime.ToString()}");
            _logger.LogInformation($"start: {startTime.ToString()}");

            //if ((currentTime > stopTime) && (currentTime < startTime))
            //{
            //    Environment.SetEnvironmentVariable("AzureWebJobs.DeleteCustomerData.Disabled", "true");
            //} 
            //else
            //{
            //    Environment.SetEnvironmentVariable("AzureWebJobs.DeleteCustomerData.Disabled", "false");
            //}
        }
    }
}
