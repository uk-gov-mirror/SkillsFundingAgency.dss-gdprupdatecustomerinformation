using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;
using Newtonsoft.Json;
using System.Text;

namespace NCS.DSS.DataUtility.Function
{
    public class DeleteDataForCustomer
    {
        private readonly ILogger<DeleteDataForCustomer> _logger;
        private readonly ICosmosDatabaseService _cosmosDatabaseService;
        private readonly ISqlDbService _sqlDbService;

        public DeleteDataForCustomer(ILogger<DeleteDataForCustomer> logger, ICosmosDatabaseService cosmosDatabaseService, ISqlDbService sqlDbService)
        {
            _logger = logger;
            _cosmosDatabaseService = cosmosDatabaseService;
            _sqlDbService = sqlDbService;
        }

        [Function(nameof(DeleteDataForCustomer))]
        public async Task Run(
            [ServiceBusTrigger("%GdprPurgeQueueName%", Connection = "ServiceBusConnectionString", AutoCompleteMessages = false)]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions
        )
        {
            _logger.LogInformation($"Function '{nameof(DeleteDataForCustomer)}' has been invoked");

            // convert queue message into usage object
            var bodyText = Encoding.UTF8.GetString(message.Body);
            RedactionQueueMessage queueBody = JsonConvert.DeserializeObject<RedactionQueueMessage>(bodyText);

            _logger.LogInformation($"Customer with ID '{queueBody.CustomerId.ToString()}' will now be processed");

            try
            {
                // PHASE 1 - DELETE DATA FROM COSMOS DB
                int actionPlanDocCountCDB = await _cosmosDatabaseService.PurgeActionPlansForCustomerAsync(queueBody.CustomerId);
                int actionDocCountCDB = await _cosmosDatabaseService.PurgeActionsForCustomerAsync(queueBody.CustomerId);
                int addressDocCountCDB = await _cosmosDatabaseService.PurgeAddressesForCustomerAsync(queueBody.CustomerId);
                int contactDetailDocCountCDB = await _cosmosDatabaseService.PurgeContactDetailsForCustomerAsync(queueBody.CustomerId);
                int digitalIdentityDocCountCDB = await _cosmosDatabaseService.PurgeDigitalIdentitiesForCustomerAsync(queueBody.CustomerId);
                int diversityDetailDocCountCDB = await _cosmosDatabaseService.PurgeDiversityDetailsForCustomerAsync(queueBody.CustomerId);
                int employmentProgressionDocCountCDB = await _cosmosDatabaseService.PurgeEmploymentProgressionsForCustomerAsync(queueBody.CustomerId);
                int goalDocCountCDB = await _cosmosDatabaseService.PurgeGoalsForCustomerAsync(queueBody.CustomerId);
                int learningProgressionDocCountCDB = await _cosmosDatabaseService.PurgeLearningProgressionsForCustomerAsync(queueBody.CustomerId);
                int outcomeDocCountCDB = await _cosmosDatabaseService.PurgeOutcomesForCustomerAsync(queueBody.CustomerId);
                int sessionDocCountCDB = await _cosmosDatabaseService.PurgeSessionsForCustomerAsync(queueBody.CustomerId);
                int subscriptionDocCountCDB = await _cosmosDatabaseService.PurgeSubscriptionsForCustomerAsync(queueBody.CustomerId);
                int transferDocCountCDB = await _cosmosDatabaseService.PurgeTransfersForCustomerAsync(queueBody.CustomerId);
                int webchatDocCountCDB = await _cosmosDatabaseService.PurgeWebchatsForCustomerAsync(queueBody.CustomerId);
                int customerDocCountCDB = await _cosmosDatabaseService.PurgeCustomerRecordAsync(queueBody.CustomerId);

                int totalDocumentCount = TotalCounter(actionPlanDocCountCDB, actionDocCountCDB, addressDocCountCDB, contactDetailDocCountCDB, digitalIdentityDocCountCDB,
                    diversityDetailDocCountCDB, employmentProgressionDocCountCDB, goalDocCountCDB, learningProgressionDocCountCDB, outcomeDocCountCDB, sessionDocCountCDB,
                    subscriptionDocCountCDB, transferDocCountCDB, webchatDocCountCDB, customerDocCountCDB);

                _logger.LogInformation($">> All Cosmos DB docs for customer '{queueBody.CustomerId.ToString()}' <<");
                _logger.LogInformation($"- Action Plans: {actionPlanDocCountCDB}");
                _logger.LogInformation($"- Actions: {actionDocCountCDB}");
                _logger.LogInformation($"- Addresses: {addressDocCountCDB}");
                _logger.LogInformation($"- Contact Details: {contactDetailDocCountCDB}");
                _logger.LogInformation($"- Digital Identities: {digitalIdentityDocCountCDB}");
                _logger.LogInformation($"- Diversity Details: {diversityDetailDocCountCDB}");
                _logger.LogInformation($"- Employment Progressions: {employmentProgressionDocCountCDB}");
                _logger.LogInformation($"- Goals: {goalDocCountCDB}");
                _logger.LogInformation($"- Learning Progressions: {learningProgressionDocCountCDB}");
                _logger.LogInformation($"- Outcomes: {outcomeDocCountCDB}");
                _logger.LogInformation($"- Sessions: {sessionDocCountCDB}");
                _logger.LogInformation($"- Subscriptions: {subscriptionDocCountCDB}");
                _logger.LogInformation($"- Transfers: {transferDocCountCDB}");
                _logger.LogInformation($"- Webchats: {webchatDocCountCDB}");
                _logger.LogInformation($"- Customer: {customerDocCountCDB}");
                _logger.LogInformation($">> Grand total : {totalDocumentCount} <<");

                // PHASE 2 - DELETE DATA FROM SQL DB
                int recordCountSqlDbAllTables = await _sqlDbService.PurgeDataItemsForCustomerAsync(queueBody.CustomerId);
                int recordCountSqlDbCustomerTable = await _sqlDbService.PurgeCustomerDataAsync(queueBody.CustomerId);

                int totalRecordCount = TotalCounter(recordCountSqlDbAllTables, recordCountSqlDbCustomerTable);

                _logger.LogInformation($">> All SQL DB records for customer '{queueBody.CustomerId.ToString()}' <<");
                _logger.LogInformation($"- Master and history tables: {recordCountSqlDbAllTables}");
                _logger.LogInformation($"- Customer record and history table: {recordCountSqlDbCustomerTable}");
                _logger.LogInformation($">> Grand total : {totalRecordCount} <<");

                // PHASE 3 - COMPLETE THE QUEUE MESSAGE
                await messageActions.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogError($"INVOCATION ERROR ({nameof(DeleteDataForCustomer)}): function has failed with exception: {ex}");
                throw;
            }

            _logger.LogInformation($"Function '{nameof(DeleteDataForCustomer)}' has finished invocation");
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
