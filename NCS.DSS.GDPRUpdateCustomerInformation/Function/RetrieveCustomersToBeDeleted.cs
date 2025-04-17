using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;

namespace NCS.DSS.DataUtility.Function
{
    public class RetrieveCustomersToBeDeleted
    {
        private readonly ILogger<RetrieveCustomersToBeDeleted> _logger;
        private readonly ISqlDbService _sqlDbService;
        private readonly IServiceBusService _serviceBusService;

        public RetrieveCustomersToBeDeleted(ILogger<RetrieveCustomersToBeDeleted> logger, ISqlDbService sqlDbService, IServiceBusService serviceBusService)
        {
            _logger = logger;
            _sqlDbService = sqlDbService;
            _serviceBusService = serviceBusService;
        }

        [Function(nameof(RetrieveCustomersToBeDeleted))]
        public async Task Run([TimerTrigger("%GdprPurgeTimerSchedule%")] TimerInfo timer)
        {
            _logger.LogInformation($"Function '{nameof(RetrieveCustomersToBeDeleted)}' has been invoked");

            try
            {
                _logger.LogInformation("Retrieving list of customer IDs which require redaction");
                List<Guid> customerIds = await _sqlDbService.RetrieveCustomerIdsAsync();

                if (customerIds.Count == 0)
                {
                    _logger.LogInformation("No customers to be redacted (data is already compliant)");
                    return;
                }

                _logger.LogInformation($"A total of '{customerIds.Count.ToString()}' customers have been identified as requiring redaction");

                _logger.LogInformation("Sending each customer ID onto a service bus queue for processing");

                foreach (var customerId in customerIds)
                {
                    DeleteCustomerQueueMessage message = new DeleteCustomerQueueMessage
                    {
                        CustomerId = customerId
                    };

                    await _serviceBusService.SendQueueMessageAsync(message);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"INVOCATION ERROR ({nameof(RetrieveCustomersToBeDeleted)}): function has failed with exception: {ex}");
                throw;
            }

            _logger.LogInformation($"Function '{nameof(RetrieveCustomersToBeDeleted)}' has finished invocation");
        }
    }
}
