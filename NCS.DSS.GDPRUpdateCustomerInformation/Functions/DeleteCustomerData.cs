using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;
using Newtonsoft.Json;
using System.Text;

namespace NCS.DSS.DataUtility.Functions
{
    public class DeleteCustomerData
    {
        private readonly ILogger<DeleteCustomerData> _logger;
        private readonly ICosmosDatabaseService _cosmosDatabaseService;
        private readonly ISqlDbService _sqlDbService;

        public DeleteCustomerData(ILogger<DeleteCustomerData> logger, ICosmosDatabaseService cosmosDatabaseService, ISqlDbService sqlDbService)
        {
            _logger = logger;
            _cosmosDatabaseService = cosmosDatabaseService;
            _sqlDbService = sqlDbService;
        }

        [Function(nameof(DeleteCustomerData))]
        public async Task Run(
            [ServiceBusTrigger("%GdprQueueName%", Connection = "ServiceBusConnectionString", AutoCompleteMessages = false)]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions
        )
        {
            _logger.LogInformation($"Function '{nameof(DeleteCustomerData)}' has been invoked");

            // convert queue message into usage object
            var bodyText = Encoding.UTF8.GetString(message.Body);
            DeleteCustomerQueueMessage queueBody = JsonConvert.DeserializeObject<DeleteCustomerQueueMessage>(bodyText);

            _logger.LogInformation($"Customer with ID '{queueBody.CustomerId.ToString()}' will now be processed");

            try
            {
                // PHASE 1 - DELETE DATA FROM COSMOS DB
                var actionPlanCosmosDB = await _cosmosDatabaseService.PurgeActionPlansForCustomerAsync(queueBody.CustomerId);
                var actionCosmosDB = await _cosmosDatabaseService.PurgeActionsForCustomerAsync(queueBody.CustomerId);
                var addressCosmosDB = await _cosmosDatabaseService.PurgeAddressesForCustomerAsync(queueBody.CustomerId);
                var contactDetailCosmosDB = await _cosmosDatabaseService.PurgeContactDetailsForCustomerAsync(queueBody.CustomerId);
                var diversityDetailCosmosDB = await _cosmosDatabaseService.PurgeDiversityDetailsForCustomerAsync(queueBody.CustomerId);
                var employmentProgressionCosmosDB = await _cosmosDatabaseService.PurgeEmploymentProgressionsForCustomerAsync(queueBody.CustomerId);
                var goalCosmosDB = await _cosmosDatabaseService.PurgeGoalsForCustomerAsync(queueBody.CustomerId);
                var interactionCosmosDB = await _cosmosDatabaseService.PurgeInteractionsForCustomerAsync(queueBody.CustomerId);
                var learningProgressionCosmosDB = await _cosmosDatabaseService.PurgeLearningProgressionsForCustomerAsync(queueBody.CustomerId);
                var outcomeCosmosDB = await _cosmosDatabaseService.PurgeOutcomesForCustomerAsync(queueBody.CustomerId);
                var sessionCosmosDB = await _cosmosDatabaseService.PurgeSessionsForCustomerAsync(queueBody.CustomerId);
                var subscriptionCosmosDB = await _cosmosDatabaseService.PurgeSubscriptionsForCustomerAsync(queueBody.CustomerId);
                var transferCosmosDB = await _cosmosDatabaseService.PurgeTransfersForCustomerAsync(queueBody.CustomerId);
                var webchatCosmosDB = await _cosmosDatabaseService.PurgeWebchatsForCustomerAsync(queueBody.CustomerId);
                var customerCosmosDB = await _cosmosDatabaseService.PurgeCustomerRecordAsync(queueBody.CustomerId);

                bool cosmosFailureHasOccurred = !actionPlanCosmosDB.processedSuccessfully
                    || !actionCosmosDB.processedSuccessfully
                    || !addressCosmosDB.processedSuccessfully
                    || !contactDetailCosmosDB.processedSuccessfully
                    || !diversityDetailCosmosDB.processedSuccessfully
                    || !employmentProgressionCosmosDB.processedSuccessfully
                    || !goalCosmosDB.processedSuccessfully
                    || !interactionCosmosDB.processedSuccessfully
                    || !learningProgressionCosmosDB.processedSuccessfully
                    || !outcomeCosmosDB.processedSuccessfully
                    || !sessionCosmosDB.processedSuccessfully
                    || !subscriptionCosmosDB.processedSuccessfully
                    || !transferCosmosDB.processedSuccessfully
                    || !webchatCosmosDB.processedSuccessfully
                    || !customerCosmosDB.processedSuccessfully;

                int totalDocumentCount = TotalCounter(actionPlanCosmosDB.impactedRecordCount, actionCosmosDB.impactedRecordCount, addressCosmosDB.impactedRecordCount,
                    contactDetailCosmosDB.impactedRecordCount, diversityDetailCosmosDB.impactedRecordCount, employmentProgressionCosmosDB.impactedRecordCount, 
                    goalCosmosDB.impactedRecordCount, interactionCosmosDB.impactedRecordCount, learningProgressionCosmosDB.impactedRecordCount, 
                    outcomeCosmosDB.impactedRecordCount, sessionCosmosDB.impactedRecordCount, subscriptionCosmosDB.impactedRecordCount, transferCosmosDB.impactedRecordCount, 
                    webchatCosmosDB.impactedRecordCount, customerCosmosDB.impactedRecordCount
                );

                string successText = cosmosFailureHasOccurred ? "no" : "yes";

                _logger.LogInformation($">> Cosmos DB purge outcome for customer '{queueBody.CustomerId.ToString()}' <<");
                _logger.LogInformation($"- Successfully processed? '{successText}'");
                _logger.LogInformation($">> Documents deleted: '{totalDocumentCount}' <<");

                // PHASE 2 - DELETE DATA FROM SQL DB
                int recordCountSqlDbAllTables = await _sqlDbService.PurgeDataItemsForCustomerAsync(queueBody.CustomerId);

                _logger.LogInformation($">> SQL DB purge outcome for customer '{queueBody.CustomerId.ToString()}' <<");
                _logger.LogInformation($"- Successfully processed? 'yes'"); // exception would be thrown if not successful
                _logger.LogInformation($">> Records deleted (data and history): {recordCountSqlDbAllTables} <<");

                if (cosmosFailureHasOccurred)
                {
                    _logger.LogWarning(
                        "Processing failure identified - moving originating message to dead letter queue. Customer ID: {customerId}"
                        , queueBody.CustomerId.ToString()
                    );
                    await messageActions.DeadLetterMessageAsync(message);
                }
                else
                {
                    // PHASE 3 - DELETE CUSTOMER RECORD FROM SQL DB
                    int recordCountSqlDbCustomerTable = await _sqlDbService.PurgeCustomerDataAsync(queueBody.CustomerId);

                    _logger.LogInformation($">> SQL DB purge outcome for customer '{queueBody.CustomerId.ToString()}' <<");
                    _logger.LogInformation($"- Successfully processed? 'yes'"); // exception would be thrown if not successful
                    _logger.LogInformation($">> Records deleted (customer): {recordCountSqlDbCustomerTable} <<");

                    _logger.LogInformation("Processing succeeded - completing message");
                    await messageActions.CompleteMessageAsync(message);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex
                    , "INVOCATION ERROR ({functionName}): function has failed with exception: {exception}. Moving originating message to dead letter queue. Customer ID: {customerId}"
                    , nameof(DeleteCustomerData)
                    , ex.Message
                    , queueBody.CustomerId.ToString()
                );
                await messageActions.DeadLetterMessageAsync(message);

                throw;
            }

            _logger.LogInformation($"Function '{nameof(DeleteCustomerData)}' has finished invocation");
        }

        private static int TotalCounter(params int[] input)
        {
            int total = 0;

            foreach (int i in input)
            {
                total += i;
            }

            return total;
        }
    }
}
