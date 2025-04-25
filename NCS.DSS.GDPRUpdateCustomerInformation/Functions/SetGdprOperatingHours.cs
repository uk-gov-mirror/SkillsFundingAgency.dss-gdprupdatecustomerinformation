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

            //get the UK culture and return the correct date time in the correct format
            //var culture = new CultureInfo("en-GB");
            
            
            //Console.WriteLine(correctDateTime.ToString(culture));





            //DateTime today = DateTime.Now;
            //DateTime todayUTC = DateTime.Now.ToUniversalTime();

            TimeSpan startPause = new TimeSpan(7, 0, 0); // 7 AM
            TimeSpan endPause = new TimeSpan(19, 0, 0); // 7 PM
            TimeSpan datetimeAsSpan = new TimeSpan(correctDateTime.Ticks);

            //_logger.LogInformation($"NOW: {today.ToString()}");
            //_logger.LogInformation($"NOW UTC: {todayUTC.ToString()}");
            _logger.LogInformation($"START TIME: {startPause.ToString()}");
            _logger.LogInformation($"END TIME: {endPause.ToString()}");
            _logger.LogInformation($"CONVERTED TIME: {datetimeAsSpan.ToString()}");

            //DateTime currentDateTimeUTC = DateTime.Now.ToUniversalTime();
            //TimeSpan currentTime = DateTime.Now.TimeOfDay;

            //DateTime

            //if (DateTime.UtcNow >= Convert.ToDateTime((String)context.Variables["startDateTime"]) && DateTime.UtcNow <= Convert.ToDateTime((String)context.Variables["endDateTime"]))
            //{
            //    isDateWithinRange = "Yes";
            //}

            ////var dt = new DateTime(localTime.Ticks);
            ////var utc = dt.ToUniversalTime();
            ////return new TimeSpan(utc.Ticks);

            //TimeSpan stopTime = new TimeSpan(16, 10, 0);
            //TimeSpan startTime = new TimeSpan(16, 20, 0);
            //TimeSpan timespanUTC = new TimeSpan(currentDateTimeUTC.Ticks);

            //_logger.LogInformation($"CURRENT TIME: {currentTime.ToString()}");
            //_logger.LogInformation($"CURRENT TIME UTC: {currentDateTimeUTC.ToString()}");

            //_logger.LogInformation($"timespan UTC: {timespanUTC.ToString()}");
            //_logger.LogInformation($"stop: {stopTime.ToString()}");
            //_logger.LogInformation($"start: {startTime.ToString()}");

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
