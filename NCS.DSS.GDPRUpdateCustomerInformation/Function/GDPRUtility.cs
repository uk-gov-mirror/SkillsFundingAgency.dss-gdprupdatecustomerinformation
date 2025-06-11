using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Services;

namespace NCS.DSS.DataUtility.Function
{
    public class GDPRUtility
    {
        private readonly IIdentifyAndAnonymiseDataService _identifyAndAnonymiseDataService;
        private readonly ILogger<GDPRUtility> _logger;

        public GDPRUtility(IIdentifyAndAnonymiseDataService identifyAndAnonymiseDataService, ILogger<GDPRUtility> logger)
        {
            _identifyAndAnonymiseDataService = identifyAndAnonymiseDataService;
            _logger = logger;
        }

        /*
        In case of failed execution, FixedDelayRetry will retry up to three times after a 30 minute delay interval.
        GDPRUtility runs at 2am on the 1st of the month, only in May and November, as defined in NCRONTAB syntax https://crontab.cronhub.io/:
        {seconds} {minutes} {hours} {day of month} {month} {day-of-week}
        */

        [Function(nameof(GDPRUtility))]
        [FixedDelayRetry(3, "00:30:00")] 
        public async Task<IActionResult> RunAsync([TimerTrigger("0 2 1 5,11 *")] TimerInfo timer)
        {
            const string functionName = nameof(GDPRUtility);

            _logger.LogInformation("{FunctionName} has been invoked", functionName);
            
            try
            {
                _logger.LogInformation("Attempting to retrieve list of customer IDs");
                List<Guid> customerIds = await _identifyAndAnonymiseDataService.ReturnCustomerIds();

                if (customerIds.Count == 0)
                {
                    _logger.LogInformation("All customers are GDPR compliant");
                    return new NoContentResult();
                }

                _logger.LogInformation("{Count} customer ID(s) have been identified as being non-compliant with GDPR", customerIds.Count.ToString());

                _logger.LogInformation("Attempting to anonymise data in SQL DB");
                await _identifyAndAnonymiseDataService.AnonymiseData();
                _logger.LogInformation("Successfully anonymised data in SQL DB");

                _logger.LogInformation("Attempting to delete documents in CosmosDB");
                await _identifyAndAnonymiseDataService.DeleteCustomersFromCosmos(customerIds);
                _logger.LogInformation("Successfully deleted documents in CosmosDB");

                _logger.LogInformation("{FunctionName} has finished invoking successfully", functionName);

                return new OkResult();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{FunctionName} has failed with error: {ErrorMessage}", functionName, ex.Message);
                throw;
            }
        }
    }
}
