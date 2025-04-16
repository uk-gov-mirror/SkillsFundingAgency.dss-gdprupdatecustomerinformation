using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;

namespace NCS.DSS.DataUtility.Services
{
    public class CosmosDatabaseService : ICosmosDatabaseService
    {
        private readonly CosmosClient _cosmosDbClient;
        private readonly ILogger<CosmosDatabaseService> _logger;

        /*private const string ActionsCosmosDb = "actions";
        private const string AddressCosmosDb = "addresses";
        private const string ContactCosmosDb = "contacts";
        private const string CustomerCosmosDb = "customers";
        private const string DigitalIdentityCosmosDb = "digitalidentities";
        private const string DiversityDetailsCosmosDb = "diversitydetails";
        private const string EmploymentProgressionCosmosDb = "employmentprogressions";
        private const string GoalsCosmosDb = "goals";
        private const string LearningProgressionCosmosDb = "learningprogressions";
        private const string OutcomesCosmosDb = "outcomes";
        private const string SessionCosmosDb = "sessions";
        private const string SubscriptionsCosmosDb = "subscriptions";
        private const string TransferCosmosDb = "transfers";
        private const string WebchatsCosmosDb = "webchats";*/

        public CosmosDatabaseService(CosmosClient cosmosClient, ILogger<CosmosDatabaseService> logger)
        {
            _cosmosDbClient = cosmosClient;
            _logger = logger;
        }

        public async Task<bool> PurgeActionPlansForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("actionplans", "actionplans");
            List<ActionPlan> actionPlans = await RetrieveActionPlansForCustomerAsync(customerId, cosmosDbContainer);

            /*foreach (var actionPlan in actionPlans)
            {
                bool success = await DeleteCosmosDocumentAsync(actionPlan.ActionPlanId.ToString(), cosmosDbContainer);
            }*/

            return true;
        }

        // private helper methods
        
        private async Task<bool> DeleteCosmosDocumentAsync(string documentId, Container cosmosDbContainer)
        {
            using (ResponseMessage response = await cosmosDbContainer.DeleteItemStreamAsync(documentId, PartitionKey.None))
            {
                _logger.LogInformation($"Status code returned was '{((int)response.StatusCode)} - {response.StatusCode}'");

                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning($"Document with ID '{documentId}' was not deleted from Cosmos DB");
                    return false;
                }

                _logger.LogInformation($"Document with ID '{documentId}' was successfully deleted from Cosmos DB");
                return true;
            }
        }
        
        private async Task<List<ActionPlan>> RetrieveActionPlansForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveActionPlansForCustomerAsync)}' has been invoked");
            
            List<ActionPlan> actionPlanList = new List<ActionPlan>();

            _logger.LogInformation($"Attempting to retrieve all Action Plan documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<ActionPlan> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<ActionPlan>()
                .Where(actionPlan => actionPlan.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (ActionPlan actionPlan in await setIterator.ReadNextAsync())
                    {
                        actionPlanList.Add(actionPlan);
                    }
                }

                _logger.LogInformation($"Processing complete. '{actionPlanList.Count().ToString()}' document(s) matching the criteria have been retrieved");

                return actionPlanList;
            }
        }
    }
}
