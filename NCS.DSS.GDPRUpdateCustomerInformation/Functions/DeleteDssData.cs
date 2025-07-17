using Azure.Messaging.ServiceBus;
using Google.Protobuf.Reflection;
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
                    await deleteCustomerData(message, messageActions, bodyText);
                    break;
                case string adviserdetail when bodyText.ToLower().Contains("adviserdetailid"):
                    await deleteAdviserDetailData(message, messageActions, bodyText);
                    break;
                case string collection when bodyText.ToLower().Contains("collectionid"):
                    await deleteCollectionData(message, messageActions, bodyText);
                    break;
            }

            _logger.LogInformation($"Function '{nameof(DeleteDSSData)}' has finished invocation");
        }

        public async Task deleteCustomerData(ServiceBusReceivedMessage message, ServiceBusMessageActions messageActions, string bodyText)
        {
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
                    , nameof(DeleteDSSData)
                    , ex.Message
                    , queueBody.CustomerId.ToString()
                );
                await messageActions.DeadLetterMessageAsync(message);

                throw;
            }
        }

        public async Task deleteAdviserDetailData(ServiceBusReceivedMessage message, ServiceBusMessageActions messageActions, string bodyText)
        {

            DeleteAdviserDetailQueueMessage queueBody = JsonConvert.DeserializeObject<DeleteAdviserDetailQueueMessage>(bodyText);
            _logger.LogInformation($"Adviser Detail with ID '{queueBody.AdviserDetailId.ToString()}' will now be processed");

            try
            {

                //Purge from CosmosDB
                var adviserDetailsCosmosDeletionSuccessful = await _cosmosDatabaseService.PurgeDocumentFromCosmosAsync(queueBody.AdviserDetailId, "adviserdetails", "adviserdetails");

                string successText = !adviserDetailsCosmosDeletionSuccessful ? "no" : "yes";
                _logger.LogInformation($">> Cosmos DB purge outcome for adviser detail '{queueBody.AdviserDetailId.ToString()}' <<");
                _logger.LogInformation($"- Successfully processed? '{successText}'");

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
                    _logger.LogInformation($">> SQL DB purge outcome for adviser detail '{queueBody.AdviserDetailId.ToString()}' <<");
                    _logger.LogInformation($"- Successfully processed? 'yes'"); // exception would be thrown if not successful

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
                await messageActions.DeadLetterMessageAsync(message);

                throw;
            }      
        }

        public async Task deleteCollectionData(ServiceBusReceivedMessage message, ServiceBusMessageActions messageActions, string bodyText)
        {
            DeleteCollectionQueueMessage queueBody = JsonConvert.DeserializeObject<DeleteCollectionQueueMessage>(bodyText);
            _logger.LogInformation($"Collection with ID '{queueBody.CollectionId.ToString()}' will now be processed");

            try
            {

                //Purge from CosmosDB
                var collectionsCosmosDeletionSuccessful = await _cosmosDatabaseService.PurgeDocumentFromCosmosAsync(queueBody.CollectionId, "collections", "collections");

                string successText = !collectionsCosmosDeletionSuccessful ? "no" : "yes";
                _logger.LogInformation($">> Cosmos DB purge outcome for collection '{queueBody.CollectionId.ToString()}' <<");
                _logger.LogInformation($"- Successfully processed? '{successText}'");

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
                    _logger.LogInformation($">> SQL DB purge outcome for collection '{queueBody.CollectionId.ToString()}' <<");
                    _logger.LogInformation($"- Successfully processed? 'yes'"); // exception would be thrown if not successful

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
                await messageActions.DeadLetterMessageAsync(message);

                throw;
            }

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
