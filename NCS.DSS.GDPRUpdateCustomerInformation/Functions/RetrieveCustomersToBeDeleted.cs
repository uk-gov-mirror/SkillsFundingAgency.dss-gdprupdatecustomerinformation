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

        private static int NumberOfSuccesses;
        private static int NumberOfFailures;

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

                var options = new ParallelOptions()
                {
                    MaxDegreeOfParallelism = 5
                };

                IEnumerable<Guid> customerIdEnumerable = customerIds;

                await Parallel.ForEachAsync(customerIdEnumerable, options, async (customerId, _) =>
                {
                    DeleteCustomerQueueMessage message = new DeleteCustomerQueueMessage
                    {
                        CustomerId = customerId
                    };

                    try
                    {
                        await _serviceBusService.SendQueueMessageAsync(message, QUEUE_NAME);
                        Interlocked.Increment(ref NumberOfSuccesses); // used for thread safety - https://jeremybytes.blogspot.com/2024/02/parallelforeachasync-and-exceptions.html
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"ERROR: Failed to send queue message in parallel. Exception: {ex}");
                        Interlocked.Increment(ref NumberOfFailures);
                    }
                });

                _logger.LogInformation($"Total number of queue messages SUCCESSFULLY sent: {NumberOfSuccesses}");
                _logger.LogInformation($"Total number of queue messages FAILED to be sent: {NumberOfFailures}");
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
