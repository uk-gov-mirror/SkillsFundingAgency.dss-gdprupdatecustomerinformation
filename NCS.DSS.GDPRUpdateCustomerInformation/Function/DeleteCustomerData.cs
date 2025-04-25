using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;
using Newtonsoft.Json;
using System.Text;

namespace NCS.DSS.DataUtility.Function
{
    public class DeleteCustomerData
    {
        private readonly ILogger<DeleteCustomerData> _logger;
        private readonly ICosmosDatabaseService _cosmosDatabaseService;
        private readonly ISqlDbService _sqlDbService;
        private readonly IServiceBusService _serviceBusService;

        private readonly string QUEUE_NAME = Environment.GetEnvironmentVariable("DeleteCustomerQueueName");

        public DeleteCustomerData(ILogger<DeleteCustomerData> logger, ICosmosDatabaseService cosmosDatabaseService, ISqlDbService sqlDbService, IServiceBusService serviceBusService)
        {
            _logger = logger;
            _cosmosDatabaseService = cosmosDatabaseService;
            _sqlDbService = sqlDbService;
            _serviceBusService = serviceBusService;
        }

        [Function(nameof(DeleteCustomerData))]
        public async Task Run(
            [ServiceBusTrigger("%DeleteCustomerDataQueueName%", Connection = "ServiceBusConnectionString", AutoCompleteMessages = false)]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions
        ) {
            _logger.LogInformation($"Function '{nameof(DeleteCustomerData)}' has been invoked");

            // convert queue message into usage object
            var bodyText = Encoding.UTF8.GetString(message.Body);
            DeleteCustomerQueueMessage queueBody = JsonConvert.DeserializeObject<DeleteCustomerQueueMessage>(bodyText);

            _logger.LogInformation($"Customer with ID '{queueBody.CustomerId.ToString()}' will now be processed");

            try
            {
                // PHASE 1 - DELETE DATA FROM COSMOS DB
                var actionPlanDocCountCDB = await _cosmosDatabaseService.PurgeActionPlansForCustomerAsync(queueBody.CustomerId);
                var actionDocCountCDB = await _cosmosDatabaseService.PurgeActionsForCustomerAsync(queueBody.CustomerId);
                var addressDocCountCDB = await _cosmosDatabaseService.PurgeAddressesForCustomerAsync(queueBody.CustomerId);
                var contactDetailDocCountCDB = await _cosmosDatabaseService.PurgeContactDetailsForCustomerAsync(queueBody.CustomerId);
                var digitalIdentityDocCountCDB = await _cosmosDatabaseService.PurgeDigitalIdentitiesForCustomerAsync(queueBody.CustomerId);
                var diversityDetailDocCountCDB = await _cosmosDatabaseService.PurgeDiversityDetailsForCustomerAsync(queueBody.CustomerId);
                var employmentProgressionDocCountCDB = await _cosmosDatabaseService.PurgeEmploymentProgressionsForCustomerAsync(queueBody.CustomerId);
                var goalDocCountCDB = await _cosmosDatabaseService.PurgeGoalsForCustomerAsync(queueBody.CustomerId);
                var learningProgressionDocCountCDB = await _cosmosDatabaseService.PurgeLearningProgressionsForCustomerAsync(queueBody.CustomerId);
                var outcomeDocCountCDB = await _cosmosDatabaseService.PurgeOutcomesForCustomerAsync(queueBody.CustomerId);
                var sessionDocCountCDB = await _cosmosDatabaseService.PurgeSessionsForCustomerAsync(queueBody.CustomerId);
                var subscriptionDocCountCDB = await _cosmosDatabaseService.PurgeSubscriptionsForCustomerAsync(queueBody.CustomerId);
                var transferDocCountCDB = await _cosmosDatabaseService.PurgeTransfersForCustomerAsync(queueBody.CustomerId);
                var webchatDocCountCDB = await _cosmosDatabaseService.PurgeWebchatsForCustomerAsync(queueBody.CustomerId);
                var customerDocCountCDB = await _cosmosDatabaseService.PurgeCustomerRecordAsync(queueBody.CustomerId);

                bool cosmosFailureHasOccurred = !actionPlanDocCountCDB.processedSuccessfully
                    || !actionDocCountCDB.processedSuccessfully
                    || !addressDocCountCDB.processedSuccessfully
                    || !contactDetailDocCountCDB.processedSuccessfully
                    || !digitalIdentityDocCountCDB.processedSuccessfully
                    || !diversityDetailDocCountCDB.processedSuccessfully
                    || !employmentProgressionDocCountCDB.processedSuccessfully
                    || !goalDocCountCDB.processedSuccessfully
                    || !learningProgressionDocCountCDB.processedSuccessfully
                    || !outcomeDocCountCDB.processedSuccessfully
                    || !sessionDocCountCDB.processedSuccessfully
                    || !subscriptionDocCountCDB.processedSuccessfully
                    || !transferDocCountCDB.processedSuccessfully
                    || !webchatDocCountCDB.processedSuccessfully
                    || !customerDocCountCDB.processedSuccessfully;

                int totalDocumentCount = TotalCounter(actionPlanDocCountCDB.impactedRecordCount, actionDocCountCDB.impactedRecordCount, addressDocCountCDB.impactedRecordCount, 
                    contactDetailDocCountCDB.impactedRecordCount, digitalIdentityDocCountCDB.impactedRecordCount, diversityDetailDocCountCDB.impactedRecordCount, 
                    employmentProgressionDocCountCDB.impactedRecordCount, goalDocCountCDB.impactedRecordCount, learningProgressionDocCountCDB.impactedRecordCount, 
                    outcomeDocCountCDB.impactedRecordCount, sessionDocCountCDB.impactedRecordCount, subscriptionDocCountCDB.impactedRecordCount, 
                    transferDocCountCDB.impactedRecordCount, webchatDocCountCDB.impactedRecordCount, customerDocCountCDB.impactedRecordCount
                );

                string successText = cosmosFailureHasOccurred ? "no" : "yes";

                _logger.LogInformation($">> All Cosmos DB docs for customer '{queueBody.CustomerId.ToString()}' <<");
                _logger.LogInformation($"- Successfully processed: {successText}");
                _logger.LogInformation($">> Grand total : {totalDocumentCount} <<");

                // PHASE 2 - DELETE DATA FROM SQL DB
                int recordCountSqlDbAllTables = await _sqlDbService.PurgeDataItemsForCustomerAsync(queueBody.CustomerId);

                _logger.LogInformation($">> All SQL DB records for customer '{queueBody.CustomerId.ToString()}' <<");
                _logger.LogInformation($"- Master and history tables: {recordCountSqlDbAllTables}");
                _logger.LogInformation($">> Grand total : {recordCountSqlDbAllTables} <<");

                if (cosmosFailureHasOccurred)
                {
                    _logger.LogWarning("Processing failure identified - moving originating message to dead letter queue");
                    await messageActions.DeadLetterMessageAsync(message);
                } 
                else
                {
                    _logger.LogInformation("Processing succeeded - completing message");
                    await messageActions.CompleteMessageAsync(message);

                    // queue new message to delete customer record
                    await _serviceBusService.SendQueueMessageAsync(queueBody, QUEUE_NAME);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"INVOCATION ERROR ({nameof(DeleteCustomerData)}): function has failed with exception: {ex}. Moving originating message to dead letter queue");
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
