using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;

namespace NCS.DSS.DataUtility.Function
{
    public class RetrieveCustomersToBeRedacted
    {
        private readonly ILogger<RetrieveCustomersToBeRedacted> _logger;
        private readonly ISqlDbService _sqlDbService;
        private readonly IServiceBusService _serviceBusService;

        public RetrieveCustomersToBeRedacted(ILogger<RetrieveCustomersToBeRedacted> logger, ISqlDbService sqlDbService, IServiceBusService serviceBusService)
        {
            _logger = logger;
            _sqlDbService = sqlDbService;
            _serviceBusService = serviceBusService;
        }

        // At 12:00 on day-of-month 1 (i.e every month on the first day - https://crontab.guru/)
        // 0 12 1 * *
        // */15 * * * * // every 15 minutes
        [Function(nameof(RetrieveCustomersToBeRedacted))]
        public async Task<IActionResult> Run([TimerTrigger("%RedactionTimerSchedule%")] TimerInfo timer) 
        {
            _logger.LogInformation($"Function '{nameof(RetrieveCustomersToBeRedacted)}' has been invoked");

            try
            {
                _logger.LogInformation("Start - retrieving list of customer IDs which require redaction");
                List<Guid> customerIds = await _sqlDbService.RetrieveCustomerIdsAsync();

                if (customerIds.Count == 0)
                {
                    _logger.LogInformation("End - no customers to be redacted (data is already compliant)");
                    return new NoContentResult();
                }

                _logger.LogInformation($"End - '{customerIds.Count.ToString()}' customers have been identified as requiring redaction");

                // TODO: Add each to service bus queue
                Guid testGuid = Guid.NewGuid();
                RedactionQueueMessage message = new RedactionQueueMessage
                {
                    CustomerId = testGuid
                };

                bool success = await _serviceBusService.SendQueueMessageAsync(message);

                if (success)
                {
                    return new OkResult();
                }

                return new BadRequestResult();
            }
            catch (Exception ex)
            {
                _logger.LogError($"INVOCATION ERROR ({nameof(RetrieveCustomersToBeRedacted)}): function has failed with exception: {ex}");
                throw;
            }
        }
    }
}
