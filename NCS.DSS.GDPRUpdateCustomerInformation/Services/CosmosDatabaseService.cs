using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;
using Action = NCS.DSS.DataUtility.Models.Action;

namespace NCS.DSS.DataUtility.Services
{
    public class CosmosDatabaseService : ICosmosDatabaseService
    {
        private readonly CosmosClient _cosmosDbClient;
        private readonly ILogger<CosmosDatabaseService> _logger;

        /*private const string CustomerCosmosDb = "customers";
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

        public async Task PurgeActionPlansForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("actionplans", "actionplans");
            List<ActionPlan> actionPlans = await RetrieveActionPlansForCustomerAsync(customerId, cosmosDbContainer);

            /*foreach (var actionPlan in actionPlans)
            {
                bool success = await DeleteCosmosDocumentAsync(actionPlan.ActionPlanId.ToString(), cosmosDbContainer);
            }*/
        }

        public async Task PurgeActionsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("actions", "actions");
            List<Action> actions = await RetrieveActionsForCustomerAsync(customerId, cosmosDbContainer);
        }

        public async Task PurgeAddressesForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("addresses", "addresses");
            List<Address> addresses = await RetrieveAddressesForCustomerAsync(customerId, cosmosDbContainer);
        }

        public async Task PurgeContactDetailsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("contacts", "contacts");
            List<ContactDetail> contactDetails = await RetrieveContactDetailsForCustomerAsync(customerId, cosmosDbContainer);
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

        private async Task<List<Action>> RetrieveActionsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveActionsForCustomerAsync)}' has been invoked");

            List<Action> actionList = new List<Action>();

            _logger.LogInformation($"Attempting to retrieve all Action documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<Action> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<Action>()
                .Where(action => action.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (Action action in await setIterator.ReadNextAsync())
                    {
                        actionList.Add(action);
                    }
                }

                _logger.LogInformation($"Processing complete. '{actionList.Count().ToString()}' document(s) matching the criteria have been retrieved");

                return actionList;
            }
        }

        private async Task<List<Address>> RetrieveAddressesForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveAddressesForCustomerAsync)}' has been invoked");

            List<Address> addressList = new List<Address>();

            _logger.LogInformation($"Attempting to retrieve all Address documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<Address> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<Address>()
                .Where(address => address.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (Address address in await setIterator.ReadNextAsync())
                    {
                        addressList.Add(address);
                    }
                }

                _logger.LogInformation($"Processing complete. '{addressList.Count().ToString()}' document(s) matching the criteria have been retrieved");

                return addressList;
            }
        }

        private async Task<List<ContactDetail>> RetrieveContactDetailsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveContactDetailsForCustomerAsync)}' has been invoked");

            List<ContactDetail> contactDetailList = new List<ContactDetail>();

            _logger.LogInformation($"Attempting to retrieve all Contact Detail documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<ContactDetail> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<ContactDetail>()
                .Where(contactDetail => contactDetail.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (ContactDetail contactDetail in await setIterator.ReadNextAsync())
                    {
                        contactDetailList.Add(contactDetail);
                    }
                }

                _logger.LogInformation($"Processing complete. '{contactDetailList.Count().ToString()}' document(s) matching the criteria have been retrieved");

                return contactDetailList;
            }
        }
    }
}
