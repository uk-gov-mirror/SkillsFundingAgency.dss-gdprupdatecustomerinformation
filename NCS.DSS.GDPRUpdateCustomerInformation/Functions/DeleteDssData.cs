using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;
using Newtonsoft.Json;
using System.Text;

namespace NCS.DSS.DataUtility.Functions
{
    public class DeleteDSSData
    {
        private readonly ILogger<DeleteDSSData> _logger;
        private readonly ICosmosDatabaseService _cosmosDatabaseService;
        private readonly ISqlDbService _sqlDbService;

        public DeleteDSSData(ILogger<DeleteDSSData> logger, ICosmosDatabaseService cosmosDatabaseService, ISqlDbService sqlDbService)
        {
            _logger = logger;
            _cosmosDatabaseService = cosmosDatabaseService;
            _sqlDbService = sqlDbService;
        }

        [Function(nameof(DeleteDSSData))]
        public async Task Run(
            [ServiceBusTrigger("%GdprQueueName%", Connection = "ServiceBusConnectionString", AutoCompleteMessages = false)]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions
        )
        {
            _logger.LogInformation($"Function '{nameof(DeleteDSSData)}' has been invoked");

            // convert queue message into usage object
            var bodyText = Encoding.UTF8.GetString(message.Body);

            switch (bodyText)
            {
                case string customer when bodyText.ToLower().Contains("customerid"):
                    await DeleteCustomerData(message, messageActions, bodyText);
                    break;
                case string adviserdetail when bodyText.ToLower().Contains("adviserdetailid"):
                    await DeleteAdviserDetailData(message, messageActions, bodyText);
                    break;
                case string collection when bodyText.ToLower().Contains("collectionid"):
                    await DeleteCollectionData(message, messageActions, bodyText);
                    break;
            }

            _logger.LogInformation($"Function '{nameof(DeleteDSSData)}' has finished invocation");
        }

        public async Task DeleteCustomerData(ServiceBusReceivedMessage message, ServiceBusMessageActions messageActions, string bodyText)
        {
            DeleteCustomerQueueMessage queueBody = JsonConvert.DeserializeObject<DeleteCustomerQueueMessage>(bodyText);

            _logger.LogInformation("Customer with ID '{CustomerId}' will now be processed", queueBody.CustomerId);

            try
            {
                // PHASE 1 - DELETE DATA FROM COSMOS DB
                var purgeResults = new Dictionary<string, (bool success, int count)>
                {
                    ["dss-actionplans"] = await _cosmosDatabaseService.PurgeActionPlansForCustomerAsync(queueBody.CustomerId),
                    ["dss-actions"] = await _cosmosDatabaseService.PurgeActionsForCustomerAsync(queueBody.CustomerId),
                    ["dss-addresses"] = await _cosmosDatabaseService.PurgeAddressesForCustomerAsync(queueBody.CustomerId),
                    ["dss-contacts"] = await _cosmosDatabaseService.PurgeContactDetailsForCustomerAsync(queueBody.CustomerId),
                    ["dss-diversitydetails"] = await _cosmosDatabaseService.PurgeDiversityDetailsForCustomerAsync(queueBody.CustomerId),
                    ["dss-employmentprogressions"] = await _cosmosDatabaseService.PurgeEmploymentProgressionsForCustomerAsync(queueBody.CustomerId),
                    ["dss-goals"] = await _cosmosDatabaseService.PurgeGoalsForCustomerAsync(queueBody.CustomerId),
                    ["dss-interactions"] = await _cosmosDatabaseService.PurgeInteractionsForCustomerAsync(queueBody.CustomerId),
                    ["dss-learningprogressions"] = await _cosmosDatabaseService.PurgeLearningProgressionsForCustomerAsync(queueBody.CustomerId),
                    ["dss-outcomes"] = await _cosmosDatabaseService.PurgeOutcomesForCustomerAsync(queueBody.CustomerId),
                    ["dss-sessions"] = await _cosmosDatabaseService.PurgeSessionsForCustomerAsync(queueBody.CustomerId),
                    ["dss-subscriptions"] = await _cosmosDatabaseService.PurgeSubscriptionsForCustomerAsync(queueBody.CustomerId),
                    ["dss-transfers"] = await _cosmosDatabaseService.PurgeTransfersForCustomerAsync(queueBody.CustomerId),
                    ["dss-webchats"] = await _cosmosDatabaseService.PurgeWebchatsForCustomerAsync(queueBody.CustomerId),
                    ["dss-customers"] = await _cosmosDatabaseService.PurgeCustomerRecordAsync(queueBody.CustomerId)
                };

                var failedTables = purgeResults
                    .Where(kvp => !kvp.Value.success)
                    .Select(kvp => kvp.Key)
                    .ToList();

                bool cosmosFailureHasOccurred = failedTables.Any();
                string failedTablesMessage = string.Join(", ", failedTables);

                int totalDocumentCount = purgeResults.Sum(kvp => kvp.Value.count);

                string successText = cosmosFailureHasOccurred ? "no" : "yes";

                _logger.LogInformation(">> Cosmos DB purge outcome for customer '{CustomerId}' <<", queueBody.CustomerId);
                _logger.LogInformation("- Successfully processed? '{SuccessText}'", successText);
                _logger.LogInformation(">> Documents deleted: '{TotalDocumentCount}' <<", totalDocumentCount);

                // PHASE 2 - DELETE DATA FROM SQL DB
                int recordCountSqlDbAllTables = await _sqlDbService.PurgeDataItemsForCustomerAsync(queueBody.CustomerId);

                _logger.LogInformation(">> SQL DB purge outcome for customer '{CustomerId}' <<", queueBody.CustomerId);
                _logger.LogInformation("- Successfully processed? 'yes'"); // exception would be thrown if not successful
                _logger.LogInformation(">> Records deleted (data and history): {RecordCountSqlDbAllTables} <<", recordCountSqlDbAllTables);

                if (cosmosFailureHasOccurred)
                {
                    _logger.LogWarning(
                        "Processing failure identified - moving originating message to dead letter queue. Customer ID: {CustomerId}. Tables: {FailedTables}"
                        , queueBody.CustomerId, failedTablesMessage
                    );
                    await messageActions.DeadLetterMessageAsync(message, null, "Cosmos DB purge has failed for table(s):" + failedTablesMessage);
                }
                else
                {
                    // PHASE 3 - DELETE CUSTOMER RECORD FROM SQL DB
                    int recordCountSqlDbCustomerTable = await _sqlDbService.PurgeCustomerDataAsync(queueBody.CustomerId);

                    _logger.LogInformation(">> SQL DB purge outcome for customer '{CustomerId}' <<", queueBody.CustomerId);
                    _logger.LogInformation("- Successfully processed? 'yes'"); // exception would be thrown if not successful
                    _logger.LogInformation(">> Records deleted (customer): {RecordCountSqlDbCustomerTable} <<", recordCountSqlDbCustomerTable);

                    _logger.LogInformation("Processing succeeded - completing message");
                    await messageActions.CompleteMessageAsync(message);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex
                    , "INVOCATION ERROR ({FunctionName}): function has failed with exception: {Exception}. Moving originating message to dead letter queue. Customer ID: {CustomerId}"
                    , nameof(DeleteDSSData)
                    , ex.Message
                    , queueBody.CustomerId
                );
                await messageActions.DeadLetterMessageAsync(message, null, ex.Message);

                throw;
            }
        }

        public async Task DeleteAdviserDetailData(ServiceBusReceivedMessage message, ServiceBusMessageActions messageActions, string bodyText)
        {

            DeleteAdviserDetailQueueMessage queueBody = JsonConvert.DeserializeObject<DeleteAdviserDetailQueueMessage>(bodyText);
            _logger.LogInformation("Adviser Detail with ID '{AdviserDetailId}' will now be processed", queueBody.AdviserDetailId);

            try
            {

                //Purge from CosmosDB
                var adviserDetailsCosmosDeletionSuccessful = await _cosmosDatabaseService.PurgeDocumentFromCosmosAsync(queueBody.AdviserDetailId, "adviserdetails", "adviserdetails");

                string successText = !adviserDetailsCosmosDeletionSuccessful ? "no" : "yes";
                _logger.LogInformation(">> Cosmos DB purge outcome for adviser detail '{AdviserDetailId}' <<", queueBody.AdviserDetailId);
                _logger.LogInformation("- Successfully processed? '{successText}'", successText);

                if (!adviserDetailsCosmosDeletionSuccessful)
                {
                    _logger.LogWarning(
                            "Processing failure identified - moving originating message to dead letter queue. AdviserDetail ID: {adviserdetailId}"
                            , queueBody.AdviserDetailId.ToString()
                        );
                    await messageActions.DeadLetterMessageAsync(message);
                }
                else
                {
                    //Purge from SqlDB
                    await _sqlDbService.PurgeRecordDataAsync(queueBody.AdviserDetailId, "adviserdetails");
                    _logger.LogInformation(">> SQL DB purge outcome for adviser detail '{AdviserDetailId}' <<", queueBody.AdviserDetailId);
                    _logger.LogInformation("- Successfully processed? 'yes'"); // exception would be thrown if not successful

                    _logger.LogInformation("Processing succeeded - completing message");
                    await messageActions.CompleteMessageAsync(message);
                }
            }
            catch (Exception ex) 
            {
                _logger.LogError(
                    ex
                    , "INVOCATION ERROR ({functionName}): function has failed with exception: {exception}. Moving originating message to dead letter queue. Adviser Detail ID: {adviserdetailId}"
                    , nameof(DeleteDSSData)
                    , ex.Message
                    , queueBody.AdviserDetailId.ToString()
                );
                await messageActions.DeadLetterMessageAsync(message, null, ex.Message);


                throw;
            }      
        }

        public async Task DeleteCollectionData(ServiceBusReceivedMessage message, ServiceBusMessageActions messageActions, string bodyText)
        {
            DeleteCollectionQueueMessage queueBody = JsonConvert.DeserializeObject<DeleteCollectionQueueMessage>(bodyText);
            _logger.LogInformation("Collection with ID '{CollectionId}' will now be processed", queueBody.CollectionId);

            try
            {

                //Purge from CosmosDB
                var collectionsCosmosDeletionSuccessful = await _cosmosDatabaseService.PurgeDocumentFromCosmosAsync(queueBody.CollectionId, "collections", "collections");

                string successText = !collectionsCosmosDeletionSuccessful ? "no" : "yes";
                _logger.LogInformation(">> Cosmos DB purge outcome for collection '{CollectionId}' <<", queueBody.CollectionId);
                _logger.LogInformation("- Successfully processed? '{successText}'", successText);

                if (!collectionsCosmosDeletionSuccessful)
                {
                    _logger.LogWarning(
                            "Processing failure identified - moving originating message to dead letter queue. Collection ID: {collectionId}"
                            , queueBody.CollectionId.ToString()
                        );
                    await messageActions.DeadLetterMessageAsync(message);
                }
                else
                {
                    //Purge from SqlDB
                    await _sqlDbService.PurgeRecordDataAsync(queueBody.CollectionId, "collections");
                    _logger.LogInformation(">> SQL DB purge outcome for collection '{CollectionId}' <<", queueBody.CollectionId);
                    _logger.LogInformation("- Successfully processed? 'yes'"); // exception would be thrown if not successful

                    _logger.LogInformation("Processing succeeded - completing message");
                    await messageActions.CompleteMessageAsync(message);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex
                    , "INVOCATION ERROR ({functionName}): function has failed with exception: {exception}. Moving originating message to dead letter queue. Collection ID: {collectionId}"
                    , nameof(DeleteDSSData)
                    , ex.Message
                    , queueBody.CollectionId.ToString()
                );
                await messageActions.DeadLetterMessageAsync(message, null, ex.Message);


                throw;
            }

        }
    }
}
