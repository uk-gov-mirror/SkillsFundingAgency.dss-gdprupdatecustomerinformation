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

        public async Task PurgeDigitalIdentitiesForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("digitalidentities", "digitalidentities");
            List<DigitalIdentity> digitalIdentities = await RetrieveDigitalIdentitiesForCustomerAsync(customerId, cosmosDbContainer);
        }

        public async Task PurgeDiversityDetailsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("diversitydetails", "diversitydetails");
            List<DiversityDetail> diversityDetails = await RetrieveDiversityDetailsForCustomerAsync(customerId, cosmosDbContainer);
        }

        public async Task PurgeEmploymentProgressionsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("employmentprogressions", "employmentprogressions");
            List<EmploymentProgression> employmentProgressions = await RetrieveEmploymentProgressionsForCustomerAsync(customerId, cosmosDbContainer);
        }

        public async Task PurgeGoalsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("goals", "goals");
            List<Goal> goals = await RetrieveGoalsForCustomerAsync(customerId, cosmosDbContainer);
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

        private async Task<List<DigitalIdentity>> RetrieveDigitalIdentitiesForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveDigitalIdentitiesForCustomerAsync)}' has been invoked");

            List<DigitalIdentity> digitalIdentityList = new List<DigitalIdentity>();

            _logger.LogInformation($"Attempting to retrieve all Digital Identity documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<DigitalIdentity> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<DigitalIdentity>()
                .Where(digitalIdentity => digitalIdentity.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (DigitalIdentity digitalIdentity in await setIterator.ReadNextAsync())
                    {
                        digitalIdentityList.Add(digitalIdentity);
                    }
                }

                _logger.LogInformation($"Processing complete. '{digitalIdentityList.Count().ToString()}' document(s) matching the criteria have been retrieved");

                return digitalIdentityList;
            }
        }

        private async Task<List<DiversityDetail>> RetrieveDiversityDetailsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveDiversityDetailsForCustomerAsync)}' has been invoked");

            List<DiversityDetail> diversityDetailList = new List<DiversityDetail>();

            _logger.LogInformation($"Attempting to retrieve all Diversity Detail documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<DiversityDetail> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<DiversityDetail>()
                .Where(diversityDetail => diversityDetail.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (DiversityDetail diversityDetail in await setIterator.ReadNextAsync())
                    {
                        diversityDetailList.Add(diversityDetail);
                    }
                }

                _logger.LogInformation($"Processing complete. '{diversityDetailList.Count().ToString()}' document(s) matching the criteria have been retrieved");

                return diversityDetailList;
            }
        }

        private async Task<List<EmploymentProgression>> RetrieveEmploymentProgressionsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveEmploymentProgressionsForCustomerAsync)}' has been invoked");

            List<EmploymentProgression> employmentProgressionList = new List<EmploymentProgression>();

            _logger.LogInformation($"Attempting to retrieve all Employment Progression documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<EmploymentProgression> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<EmploymentProgression>()
                .Where(employmentProgression => employmentProgression.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (EmploymentProgression employmentProgression in await setIterator.ReadNextAsync())
                    {
                        employmentProgressionList.Add(employmentProgression);
                    }
                }

                _logger.LogInformation($"Processing complete. '{employmentProgressionList.Count().ToString()}' document(s) matching the criteria have been retrieved");

                return employmentProgressionList;
            }
        }

        private async Task<List<Goal>> RetrieveGoalsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveGoalsForCustomerAsync)}' has been invoked");

            List<Goal> goalList = new List<Goal>();

            _logger.LogInformation($"Attempting to retrieve all Goal documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<Goal> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<Goal>()
                .Where(goal => goal.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (Goal goal in await setIterator.ReadNextAsync())
                    {
                        goalList.Add(goal);
                    }
                }

                _logger.LogInformation($"Processing complete. '{goalList.Count().ToString()}' document(s) matching the criteria have been retrieved");

                return goalList;
            }
        }

        /*private const string CustomerCosmosDb = "customers";
       private const string LearningProgressionCosmosDb = "learningprogressions";
       private const string OutcomesCosmosDb = "outcomes";
       private const string SessionCosmosDb = "sessions";
       private const string SubscriptionsCosmosDb = "subscriptions";
       private const string TransferCosmosDb = "transfers";
       private const string WebchatsCosmosDb = "webchats";*/
    }
}
