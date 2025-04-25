using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Globalization;

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
            //get the current UTC time
            DateTime localServerTime = DateTime.UtcNow;

            //Find out if the GMT is in daylight saving time or not.
            var info = TimeZoneInfo.FindSystemTimeZoneById("GMT Standard Time");
            var isDaylightSaving = info.IsDaylightSavingTime(localServerTime);

            //set the time to be the local server time
            var correctDateTime = localServerTime;

            //if the zone is in daylight saving add an hour.
            if (isDaylightSaving)
            {
                correctDateTime = correctDateTime.AddHours(1);
            }

            TimeSpan startPause = new TimeSpan(7, 0, 0); // 7 AM
            TimeSpan endPause = new TimeSpan(18, 0, 0); // 7 PM
            TimeSpan datetimeAsSpan2 = new TimeSpan(correctDateTime.Hour, correctDateTime.Minute, correctDateTime.Second);

            if ((datetimeAsSpan2 >= startPause) && (datetimeAsSpan2 < endPause))
            {
                Environment.SetEnvironmentVariable("AzureWebJobs.DeleteCustomerData.Disabled", "true");
                _logger.LogInformation("WITHIN OPERATIONAL HOURS - disable the trigger");
            } 
            else
            {
                Environment.SetEnvironmentVariable("AzureWebJobs.DeleteCustomerData.Disabled", "false");
                _logger.LogInformation("OUTSIDE OF OPERATIONAL HOURS - enable the trigger");
            }
        }
    }
}
