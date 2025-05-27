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

        public CosmosDatabaseService(CosmosClient cosmosClient, ILogger<CosmosDatabaseService> logger)
        {
            _cosmosDbClient = cosmosClient;
            _logger = logger;
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeActionPlansForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("actionplans", "actionplans");
            List<ActionPlan> actionPlans = await RetrieveActionPlansForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var actionPlan in actionPlans)
            {
                bool success = await DeleteCosmosDocumentAsync(actionPlan.ActionPlanId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, actionPlans.Count()); // if failureOccurred == true, processedSuccessfully needs to == false
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeActionsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("actions", "actions");
            List<Action> actions = await RetrieveActionsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var action in actions)
            {
                bool success = await DeleteCosmosDocumentAsync(action.ActionId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, actions.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeAddressesForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("addresses", "addresses");
            List<Address> addresses = await RetrieveAddressesForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var address in addresses)
            {
                bool success = await DeleteCosmosDocumentAsync(address.AddressId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, addresses.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeContactDetailsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("contacts", "contacts");
            List<ContactDetail> contactDetails = await RetrieveContactDetailsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var contactDetail in contactDetails)
            {
                bool success = await DeleteCosmosDocumentAsync(contactDetail.ContactId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, contactDetails.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeCustomerRecordAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("customers", "customers");
            Customer customer = await RetrieveCustomerRecordAsync(customerId, cosmosDbContainer);

            if (customer == null)
            {
                return (true, 0); // if the customer doesn't exist in CDB, then this shouldn't constitute a failure
            }

            bool success = await DeleteCosmosDocumentAsync(customer.CustomerId.ToString(), cosmosDbContainer);

            if (success)
            {
                return (true, 1);
            }

            return (false, 0);
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeDiversityDetailsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("diversitydetails", "diversitydetails");
            List<DiversityDetail> diversityDetails = await RetrieveDiversityDetailsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var diversityDetail in diversityDetails)
            {
                bool success = await DeleteCosmosDocumentAsync(diversityDetail.DiversityId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, diversityDetails.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeEmploymentProgressionsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("employmentprogressions", "employmentprogressions");
            List<EmploymentProgression> employmentProgressions = await RetrieveEmploymentProgressionsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var employmentProgression in employmentProgressions)
            {
                bool success = await DeleteCosmosDocumentAsync(employmentProgression.EmploymentProgressionId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, employmentProgressions.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeGoalsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("goals", "goals");
            List<Goal> goals = await RetrieveGoalsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var goal in goals)
            {
                bool success = await DeleteCosmosDocumentAsync(goal.GoalId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, goals.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeInteractionsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("interactions", "interactions");
            List<Interaction> interactions = await RetrieveInteractionsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var interaction in interactions)
            {
                bool success = await DeleteCosmosDocumentAsync(interaction.InteractionId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, interactions.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeLearningProgressionsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("learningprogressions", "learningprogressions");
            List<LearningProgression> learningProgressions = await RetrieveLearningProgressionsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var learningProgression in learningProgressions)
            {
                bool success = await DeleteCosmosDocumentAsync(learningProgression.LearningProgressionId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, learningProgressions.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeOutcomesForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("outcomes", "outcomes");
            List<Outcome> outcomes = await RetrieveOutcomesForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var outcome in outcomes)
            {
                bool success = await DeleteCosmosDocumentAsync(outcome.OutcomeId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, outcomes.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeSessionsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("sessions", "sessions");
            List<Session> sessions = await RetrieveSessionsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var session in sessions)
            {
                bool success = await DeleteCosmosDocumentAsync(session.SessionId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, sessions.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeSubscriptionsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("subscriptions", "subscriptions");
            List<Subscription> subscriptions = await RetrieveSubscriptionsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var subscription in subscriptions)
            {
                bool success = await DeleteCosmosDocumentAsync(subscription.SubscriptionId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, subscriptions.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeTransfersForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("transfers", "transfers");
            List<Transfer> transfers = await RetrieveTransferForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var transfer in transfers)
            {
                bool success = await DeleteCosmosDocumentAsync(transfer.TransferId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, transfers.Count());
        }

        public async Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeWebchatsForCustomerAsync(Guid customerId)
        {
            Container cosmosDbContainer = _cosmosDbClient.GetContainer("webchats", "webchats");
            List<Webchat> webchats = await RetrieveWebchatsForCustomerAsync(customerId, cosmosDbContainer);
            bool failureOccurred = false;

            foreach (var webchat in webchats)
            {
                bool success = await DeleteCosmosDocumentAsync(webchat.WebChatId.ToString(), cosmosDbContainer);

                if (!success)
                {
                    failureOccurred = true;
                }
            }

            return (!failureOccurred, webchats.Count());
        }

        // Used by the Cosmos Bulk Delete function
        public async Task DeleteGenericRecordsFromContainer(string databaseName, string containerName, string field, string value, bool int_bool)
        {
            _logger.LogInformation($"Attempting to retrieve Cosmos records/documents with value '{value}' for field '{field}' from container '{containerName}' from within database '{databaseName}'");

            Container cosmosDbContainer = _cosmosDbClient.GetContainer(databaseName, containerName);

            // handles string/int parsing based on the int_bool flag
            string queryString;
            if (int_bool)
            {
                queryString = $"SELECT * FROM c WHERE c.{field} = {value}";
            }
            else
            {
                queryString = $"SELECT * FROM c WHERE c.{field} = @value";
            }

            QueryDefinition queryDefinition = new QueryDefinition(queryString).WithParameter("@value", value);

            FeedIterator<dynamic> resultSet = cosmosDbContainer.GetItemQueryIterator<dynamic>(queryDefinition);

            List<string> documentIds = new List<string>();

            while (resultSet.HasMoreResults)
            {
                FeedResponse<dynamic> documentRetrievalRequest = await resultSet.ReadNextAsync();
                foreach (var document in documentRetrievalRequest)
                {
                    documentIds.Add(Convert.ToString(document.id));
                }
            }

            if (documentIds.Count > 0)
            {
                _logger.LogInformation($"Container '{containerName}' has a total of {documentIds.Count.ToString()} matching records/documents");
                int totalDeleted = 0;

                foreach (var documentId in documentIds)
                {
                    using (ResponseMessage deleteRequestResponse = await cosmosDbContainer.DeleteItemStreamAsync(documentId, PartitionKey.None))
                    {
                        if (!deleteRequestResponse.IsSuccessStatusCode)
                        {
                            _logger.LogWarning($"Failed to delete Cosmos record/document with documentId: '{documentId}'. Response code: {deleteRequestResponse.StatusCode.ToString()}. Error: {deleteRequestResponse.ErrorMessage}");
                        }
                        else
                        {
                            totalDeleted++;
                        }
                    }
                }

                _logger.LogInformation($"{totalDeleted.ToString()} / {documentIds.Count.ToString()} '{containerName}' records/documents have been deleted successfully");
            }
            else
            {
                _logger.LogWarning($"No Cosmos records/documents with value '{value}' for field '{field}' were found");
            }
        }

        // private helper methods

        private async Task<bool> DeleteCosmosDocumentAsync(string documentId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(DeleteCosmosDocumentAsync)}' has been invoked");

            using (ResponseMessage response = await cosmosDbContainer.DeleteItemStreamAsync(documentId, PartitionKey.None))
            {
                _logger.LogInformation($"Status code returned was '{((int)response.StatusCode)} - {response.StatusCode}'");

                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning($"Document with ID '{documentId}' was not deleted from Cosmos DB");
                    return false;
                }

                _logger.LogInformation($"Document with ID '{documentId}' was successfully deleted from Cosmos DB");
            }

            return true;
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

                _logger.LogInformation("Processing complete");
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

                _logger.LogInformation("Processing complete");
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

                _logger.LogInformation("Processing complete");
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

                _logger.LogInformation("Processing complete");
                return contactDetailList;
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

                _logger.LogInformation("Processing complete");
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

                _logger.LogInformation("Processing complete");
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

                _logger.LogInformation("Processing complete");
                return goalList;
            }
        }

        private async Task<List<Interaction>> RetrieveInteractionsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveInteractionsForCustomerAsync)}' has been invoked");

            List<Interaction> interactionList = new List<Interaction>();

            _logger.LogInformation($"Attempting to retrieve all Interaction documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<Interaction> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<Interaction>()
                .Where(interaction => interaction.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (Interaction interaction in await setIterator.ReadNextAsync())
                    {
                        interactionList.Add(interaction);
                    }
                }

                _logger.LogInformation("Processing complete");
                return interactionList;
            }
        }

        private async Task<List<LearningProgression>> RetrieveLearningProgressionsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveLearningProgressionsForCustomerAsync)}' has been invoked");

            List<LearningProgression> learningProgressionList = new List<LearningProgression>();

            _logger.LogInformation($"Attempting to retrieve all Learning Progression documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<LearningProgression> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<LearningProgression>()
                .Where(learningProgression => learningProgression.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (LearningProgression learningProgression in await setIterator.ReadNextAsync())
                    {
                        learningProgressionList.Add(learningProgression);
                    }
                }

                _logger.LogInformation("Processing complete");
                return learningProgressionList;
            }
        }

        private async Task<List<Outcome>> RetrieveOutcomesForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveOutcomesForCustomerAsync)}' has been invoked");

            List<Outcome> outcomeList = new List<Outcome>();

            _logger.LogInformation($"Attempting to retrieve all Outcome documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<Outcome> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<Outcome>()
                .Where(outcome => outcome.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (Outcome outcome in await setIterator.ReadNextAsync())
                    {
                        outcomeList.Add(outcome);
                    }
                }

                _logger.LogInformation("Processing complete");
                return outcomeList;
            }
        }

        private async Task<List<Session>> RetrieveSessionsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveSessionsForCustomerAsync)}' has been invoked");

            List<Session> sessionList = new List<Session>();

            _logger.LogInformation($"Attempting to retrieve all Session documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<Session> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<Session>()
                .Where(session => session.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (Session session in await setIterator.ReadNextAsync())
                    {
                        sessionList.Add(session);
                    }
                }

                _logger.LogInformation("Processing complete");
                return sessionList;
            }
        }

        private async Task<List<Subscription>> RetrieveSubscriptionsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveSubscriptionsForCustomerAsync)}' has been invoked");

            List<Subscription> subscriptionList = new List<Subscription>();

            _logger.LogInformation($"Attempting to retrieve all Subscription documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<Subscription> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<Subscription>()
                .Where(subscription => subscription.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (Subscription subscription in await setIterator.ReadNextAsync())
                    {
                        subscriptionList.Add(subscription);
                    }
                }

                _logger.LogInformation("Processing complete");
                return subscriptionList;
            }
        }

        private async Task<List<Transfer>> RetrieveTransferForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveTransferForCustomerAsync)}' has been invoked");

            List<Transfer> transferList = new List<Transfer>();

            _logger.LogInformation($"Attempting to retrieve all Transfer documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<Transfer> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<Transfer>()
                .Where(transfer => transfer.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (Transfer transfer in await setIterator.ReadNextAsync())
                    {
                        transferList.Add(transfer);
                    }
                }

                _logger.LogInformation("Processing complete");
                return transferList;
            }
        }

        private async Task<List<Webchat>> RetrieveWebchatsForCustomerAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveWebchatsForCustomerAsync)}' has been invoked");

            List<Webchat> webchatList = new List<Webchat>();

            _logger.LogInformation($"Attempting to retrieve all Webchat documents with CustomerId '{customerId}' from Cosmos DB");

            using (FeedIterator<Webchat> setIterator = cosmosDbContainer
                .GetItemLinqQueryable<Webchat>()
                .Where(webchat => webchat.CustomerId == customerId)
                .ToFeedIterator()
            )
            {
                while (setIterator.HasMoreResults)
                {
                    foreach (Webchat webchat in await setIterator.ReadNextAsync())
                    {
                        webchatList.Add(webchat);
                    }
                }

                _logger.LogInformation("Processing complete");
                return webchatList;
            }
        }

        private async Task<Customer> RetrieveCustomerRecordAsync(Guid customerId, Container cosmosDbContainer)
        {
            _logger.LogInformation($"Method '{nameof(RetrieveCustomerRecordAsync)}' has been invoked");
            _logger.LogInformation($"Attempting to retrieve customer document with ID '{customerId}' from Cosmos DB");

            using (ResponseMessage response = await cosmosDbContainer.ReadItemStreamAsync(customerId.ToString(), PartitionKey.None))
            {
                _logger.LogInformation($"Status code returned was '{((int)response.StatusCode)} - {response.StatusCode}'");

                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning($"No customer document with ID '{customerId}' could be found within Cosmos DB");
                    return null;
                }

                _logger.LogInformation($"A customer document with ID '{customerId}' was found within Cosmos DB");
                _logger.LogInformation("Processing complete");

                return new Customer { CustomerId = customerId };
            }
        }
    }
}
