using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;
using Newtonsoft.Json;
using System.Text;

namespace NCS.DSS.DataUtility.Function
{
    public class DeleteCustomer
    {
        private readonly ILogger<DeleteCustomer> _logger;
        private readonly ISqlDbService _sqlDbService;

        public DeleteCustomer(ILogger<DeleteCustomer> logger, ISqlDbService sqlDbService)
        {
            _logger = logger;
            _sqlDbService = sqlDbService;
        }

        [Function(nameof(DeleteCustomer))]
        public async Task Run(
            [ServiceBusTrigger("%DeleteCustomerQueueName%", Connection = "ServiceBusConnectionString", AutoCompleteMessages = false)]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions
        ) {
            _logger.LogInformation($"Function '{nameof(DeleteCustomer)}' has been invoked");

            // convert queue message into usage object
            var bodyText = Encoding.UTF8.GetString(message.Body);
            DeleteCustomerQueueMessage queueBody = JsonConvert.DeserializeObject<DeleteCustomerQueueMessage>(bodyText);

            _logger.LogInformation($"Customer with ID '{queueBody.CustomerId.ToString()}' will now be processed");

            try
            {
                // PHASE 3 - DELETE CUSTOMER RECORD FROM SQL DB
                int recordCountSqlDbCustomerTable = await _sqlDbService.PurgeCustomerDataAsync(queueBody.CustomerId);

                _logger.LogInformation($">> SQL DB customer records for '{queueBody.CustomerId.ToString()}' <<");
                _logger.LogInformation($"- Customer record and history table: {recordCountSqlDbCustomerTable}");
                _logger.LogInformation($">> Grand total : {recordCountSqlDbCustomerTable} <<");

                // COMPLETE THE QUEUE MESSAGE
                await messageActions.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogError($"INVOCATION ERROR ({nameof(DeleteCustomer)}): function has failed with exception: {ex}. Moving originating message to dead letter queue");
                await messageActions.DeadLetterMessageAsync(message);

                throw;
            }

            _logger.LogInformation($"Function '{nameof(DeleteCustomer)}' has finished invocation");
        }
    }
}
