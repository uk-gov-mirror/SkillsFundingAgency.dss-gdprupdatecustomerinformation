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
        )
        {
            _logger.LogInformation($"Function '{nameof(RedactDataForCustomer)}' has been invoked");

            // convert queue message into usage object
            var bodyText = Encoding.UTF8.GetString(message.Body);
            RedactionQueueMessage queueBody = JsonConvert.DeserializeObject<RedactionQueueMessage>(bodyText);

            _logger.LogInformation($"Customer with ID '{queueBody.CustomerId.ToString()}' will now be processed");

            try
            {
                int actionPlanDocCount = await _cosmosDatabaseService.PurgeActionPlansForCustomerAsync(queueBody.CustomerId);
                int actionDocCount = await _cosmosDatabaseService.PurgeActionsForCustomerAsync(queueBody.CustomerId);
                int addressDocCount = await _cosmosDatabaseService.PurgeAddressesForCustomerAsync(queueBody.CustomerId);
                int contactDetailDocCount = await _cosmosDatabaseService.PurgeContactDetailsForCustomerAsync(queueBody.CustomerId);
                int digitalIdentityDocCount = await _cosmosDatabaseService.PurgeDigitalIdentitiesForCustomerAsync(queueBody.CustomerId);
                int diversityDetailDocCount = await _cosmosDatabaseService.PurgeDiversityDetailsForCustomerAsync(queueBody.CustomerId);
                int employmentProgressionDocCount = await _cosmosDatabaseService.PurgeEmploymentProgressionsForCustomerAsync(queueBody.CustomerId);
                int goalDocCount = await _cosmosDatabaseService.PurgeGoalsForCustomerAsync(queueBody.CustomerId);
                int learningProgressionDocCount = await _cosmosDatabaseService.PurgeLearningProgressionsForCustomerAsync(queueBody.CustomerId);
                int outcomeDocCount = await _cosmosDatabaseService.PurgeOutcomesForCustomerAsync(queueBody.CustomerId);
                int sessionDocCount = await _cosmosDatabaseService.PurgeSessionsForCustomerAsync(queueBody.CustomerId);
                int subscriptionDocCount = await _cosmosDatabaseService.PurgeSubscriptionsForCustomerAsync(queueBody.CustomerId);
                int transferDocCount = await _cosmosDatabaseService.PurgeTransfersForCustomerAsync(queueBody.CustomerId);
                int webchatDocCount = await _cosmosDatabaseService.PurgeWebchatsForCustomerAsync(queueBody.CustomerId);

                int totalDocumentCount = TotalCounter(actionPlanDocCount, actionDocCount, addressDocCount, contactDetailDocCount, digitalIdentityDocCount,
                    diversityDetailDocCount, employmentProgressionDocCount, goalDocCount, learningProgressionDocCount, outcomeDocCount, sessionDocCount,
                    subscriptionDocCount, transferDocCount, webchatDocCount);

                _logger.LogInformation($">> All docs for customer '{queueBody.CustomerId.ToString()}' <<");
                _logger.LogInformation($"- Action Plans: {actionPlanDocCount}");
                _logger.LogInformation($"- Actions: {actionDocCount}");
                _logger.LogInformation($"- Addresses: {addressDocCount}");
                _logger.LogInformation($"- Contact Details: {contactDetailDocCount}");
                _logger.LogInformation($"- Digital Identities: {digitalIdentityDocCount}");
                _logger.LogInformation($"- Diversity Details: {diversityDetailDocCount}");
                _logger.LogInformation($"- Employment Progressions: {employmentProgressionDocCount}");
                _logger.LogInformation($"- Goals: {goalDocCount}");
                _logger.LogInformation($"- Learning Progressions: {learningProgressionDocCount}");
                _logger.LogInformation($"- Outcomes: {outcomeDocCount}");
                _logger.LogInformation($"- Sessions: {sessionDocCount}");
                _logger.LogInformation($"- Subscriptions: {subscriptionDocCount}");
                _logger.LogInformation($"- Transfers: {transferDocCount}");
                _logger.LogInformation($"- Webchats: {webchatDocCount}");
                _logger.LogInformation($">> Grand total : {totalDocumentCount} <<");

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
