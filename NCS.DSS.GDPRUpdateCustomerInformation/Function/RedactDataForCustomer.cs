using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;
using Newtonsoft.Json;
using System.Text;

namespace NCS.DSS.DataUtility.Function
{
    public class RedactDataForCustomer
    {
        private readonly ILogger<RedactDataForCustomer> _logger;
        private readonly ICosmosDatabaseService _cosmosDatabaseService;

        public RedactDataForCustomer(ILogger<RedactDataForCustomer> logger, ICosmosDatabaseService cosmosDatabaseService)
        {
            _logger = logger;
            _cosmosDatabaseService = cosmosDatabaseService;
        }

        [Function(nameof(RedactDataForCustomer))]
        public async Task Run(
            [ServiceBusTrigger("%RedactionQueueName%", Connection = "ServiceBusConnectionString", AutoCompleteMessages = false)] 
            ServiceBusReceivedMessage message, 
            ServiceBusMessageActions messageActions
        ) {
            _logger.LogInformation($"Function '{nameof(RedactDataForCustomer)}' has been invoked");

            // convert queue message into usage object
            var bodyText = Encoding.UTF8.GetString(message.Body);
            RedactionQueueMessage queueBody = JsonConvert.DeserializeObject<RedactionQueueMessage>(bodyText);

            _logger.LogInformation($"Customer with ID '{queueBody.CustomerId.ToString()}' will now be processed");

            try
            {
                await _cosmosDatabaseService.PurgeActionPlansForCustomerAsync(queueBody.CustomerId);
                await _cosmosDatabaseService.PurgeActionsForCustomerAsync(queueBody.CustomerId);
                await _cosmosDatabaseService.PurgeAddressesForCustomerAsync(queueBody.CustomerId);
                await _cosmosDatabaseService.PurgeContactDetailsForCustomerAsync(queueBody.CustomerId);

                // Complete the message
                await messageActions.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogError($"INVOCATION ERROR ({nameof(RedactDataForCustomer)}): function has failed with exception: {ex}");
                throw;
            }

            _logger.LogInformation($"Function '{nameof(RedactDataForCustomer)}' has finished invocation");
        }
    }
}
