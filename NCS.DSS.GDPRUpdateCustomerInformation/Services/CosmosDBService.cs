using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace NCS.DSS.DataUtility.Services
{
    public class CosmosDBService : ICosmosDBService
    {
        private readonly CosmosClient _cosmosDbClient;
        private readonly ILogger<CosmosDBService> _logger;

        private const string ActionPlansCosmosDb = "actionplans";
        private const string ActionsCosmosDb = "actions";
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
        private const string WebchatsCosmosDb = "webchats";

        public CosmosDBService(CosmosClient cosmosClient, ILogger<CosmosDBService> logger)
        {
            _cosmosDbClient = cosmosClient;
            _logger = logger;
        }

        public async Task DeleteRecordsForCustomer(Guid customerId)
        {
            _logger.LogInformation("{FunctionName} function has been invoked", nameof(DeleteRecordsForCustomer));

            var actionPlansTask = DeleteDocumentFromContainer(customerId, ActionPlansCosmosDb, ActionPlansCosmosDb);
            var actionsTask = DeleteDocumentFromContainer(customerId, ActionsCosmosDb, ActionsCosmosDb);
            var addressesTask = DeleteDocumentFromContainer(customerId, AddressCosmosDb, AddressCosmosDb);
            var contactsTask = DeleteDocumentFromContainer(customerId, ContactCosmosDb, ContactCosmosDb);
            var employmentProgressionTask = DeleteDocumentFromContainer(customerId, EmploymentProgressionCosmosDb, EmploymentProgressionCosmosDb);
            var goalsTask = DeleteDocumentFromContainer(customerId, GoalsCosmosDb, GoalsCosmosDb);
            var webchatsTask = DeleteDocumentFromContainer(customerId, WebchatsCosmosDb, WebchatsCosmosDb);
            var digitalIdentityTask = DeleteDocumentFromContainer(customerId, DigitalIdentityCosmosDb, DigitalIdentityCosmosDb);
            var diverityDetailsTask = DeleteDocumentFromContainer(customerId, DiversityDetailsCosmosDb, DiversityDetailsCosmosDb);
            var learningProgressionsTask = DeleteDocumentFromContainer(customerId, LearningProgressionCosmosDb, LearningProgressionCosmosDb);
            var outcomesTask = DeleteDocumentFromContainer(customerId, OutcomesCosmosDb, OutcomesCosmosDb);
            var sessionsTask = DeleteDocumentFromContainer(customerId, SessionCosmosDb, SessionCosmosDb);
            var subscriptionsTask = DeleteDocumentFromContainer(customerId, SubscriptionsCosmosDb, SubscriptionsCosmosDb);
            var transfersTask = DeleteDocumentFromContainer(customerId, TransferCosmosDb, TransferCosmosDb);

            await Task.WhenAll(actionPlansTask, actionsTask, addressesTask, contactsTask, employmentProgressionTask,
                goalsTask, webchatsTask, digitalIdentityTask, diverityDetailsTask, learningProgressionsTask,
                outcomesTask, sessionsTask, subscriptionsTask, transfersTask);

            await DeleteDocumentFromContainer(customerId, CustomerCosmosDb, CustomerCosmosDb);

            _logger.LogInformation("{FunctionName} function has finished invoking", nameof(DeleteRecordsForCustomer));
        }

        private async Task DeleteDocumentFromContainer(Guid customerId, string databaseName, string containerName)
        {
            _logger.LogInformation("Attempting to retrieve documents associated with customer [{CustomerId}] from container '{ContainerName}' in database '{DatabaseName}'", customerId.ToString(), containerName, databaseName);

            Container cosmosDbContainer = _cosmosDbClient.GetContainer(databaseName, containerName);

            QueryDefinition queryDefinition = containerName == CustomerCosmosDb ?
                new QueryDefinition("SELECT * FROM c WHERE c.id = @customerId").WithParameter("@customerId", customerId)
                : new QueryDefinition("SELECT * FROM c WHERE c.CustomerId = @customerId").WithParameter("@customerId", customerId);

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
                _logger.LogInformation("Customer [{CustomerId}] has {DocumentIdCount} document(s) in '{ContainerName}'", customerId.ToString(), documentIds.Count.ToString(), containerName);
                int totalDeleted = 0;

                foreach (var documentId in documentIds)
                {
                    using ResponseMessage deleteRequestResponse = await cosmosDbContainer.DeleteItemStreamAsync(documentId, PartitionKey.None);
                    if (!deleteRequestResponse.IsSuccessStatusCode)
                    {
                        _logger.LogWarning("Failed to delete document [{DocumentId}] in '{ContainerName}'. Response code: {ResponseCode}. Error: {ErrorMessage}", documentId, containerName, deleteRequestResponse.StatusCode.ToString(), deleteRequestResponse.ErrorMessage);
                    }
                    else
                    {
                        totalDeleted++;
                    }
                }

                _logger.LogInformation("{totalDeleted} / {DocumentIdsCount} documents have been deleted successfully in '{ContainerName}'", totalDeleted.ToString(), documentIds.Count.ToString(), containerName);
            }
            else
            {
                _logger.LogInformation("No documents in '{ContainerName}' were found for customer [{CustomerId}]", containerName, customerId.ToString());
            }
        }

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

    }
}
