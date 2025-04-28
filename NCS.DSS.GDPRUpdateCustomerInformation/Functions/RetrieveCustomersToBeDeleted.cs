using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;

namespace NCS.DSS.DataUtility.Functions
{
    public class RetrieveCustomersToBeDeleted
    {
        private readonly ILogger<RetrieveCustomersToBeDeleted> _logger;
        private readonly ISqlDbService _sqlDbService;
        private readonly IServiceBusService _serviceBusService;

        private readonly string QUEUE_NAME = Environment.GetEnvironmentVariable("GdprQueueName");

        public RetrieveCustomersToBeDeleted(ILogger<RetrieveCustomersToBeDeleted> logger, ISqlDbService sqlDbService, IServiceBusService serviceBusService)
        {
            _logger = logger;
            _sqlDbService = sqlDbService;
            _serviceBusService = serviceBusService;
        }

        [Function(nameof(RetrieveCustomersToBeDeleted))]
        public async Task Run([TimerTrigger("%GdprTimerSchedule%")] TimerInfo timer)
        {
            _logger.LogInformation($"Function '{nameof(RetrieveCustomersToBeDeleted)}' has been invoked");

            try
            {
                _logger.LogInformation("Retrieving list of customer IDs which require deletion");
                List<Guid> customerIds = await _sqlDbService.RetrieveCustomerIdsAsync();

                if (customerIds.Count == 0)
                {
                    _logger.LogInformation("No customers to be deleted (data is already GDPR compliant)");
                    return;
                }

                _logger.LogInformation($"A total of '{customerIds.Count.ToString()}' customers have been identified as requiring deletion");

                _logger.LogInformation("Sending each customer ID onto a service bus queue for processing");

                foreach (var customerId in customerIds)
                {
                    DeleteCustomerQueueMessage message = new DeleteCustomerQueueMessage
                    {
                        CustomerId = customerId
                    };

                    await _serviceBusService.SendQueueMessageAsync(message, QUEUE_NAME);
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
