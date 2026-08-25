using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NCS.DSS.DataUtility.Functions
{
    public class DssDecomPoc
    {
        private readonly ILogger<DssDecomPoc> _logger;
        private readonly ICosmosDatabaseService _cosmosDatabaseService;
        private readonly CosmosClient _cosmosDbClient;

        public DssDecomPoc(ILogger<DssDecomPoc> logger, ICosmosDatabaseService cosmosDatabaseService, CosmosClient cosmosClient)
        {
            _logger = logger;
            _cosmosDatabaseService = cosmosDatabaseService;
            _cosmosDbClient = cosmosClient;
        }

        [Function("SetTTLOnItem")]
        public async Task<IActionResult> SetTTLOnItem([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequest req)
        {

            return new OkResult();
        }

        [Function("EnableTTLInContainers")]
        public async Task<IActionResult> EnableTTLInContainers([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequest req)
        {
            using (FeedIterator<DatabaseProperties> dbIterator = _cosmosDbClient.GetDatabaseQueryIterator<DatabaseProperties>())
            {
                while (dbIterator.HasMoreResults)
                {
                    foreach (DatabaseProperties db in await dbIterator.ReadNextAsync())
                    {
                        Database database2 = _cosmosDbClient.GetDatabase(db.Id);
                        FeedIterator<ContainerProperties> iterator = database2.GetContainerQueryIterator<ContainerProperties>();
                        while (iterator.HasMoreResults)
                        {
                            foreach (ContainerProperties c in await iterator.ReadNextAsync())
                            {
                                await enableTTL(db, c);
                            }
                        }
                    }
                }
            }

            return new OkResult();
        }

        private async Task enableTTL(DatabaseProperties dbProperties, ContainerProperties cProperties)
        {
            var db = _cosmosDbClient.GetDatabase(dbProperties.Id);
            var container = db.GetContainer(cProperties.Id);
            cProperties.DefaultTimeToLive = -1;
            await container.ReplaceContainerAsync(cProperties);
        }
    }
}
