using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using System.Data;

namespace NCS.DSS.DataUtility.Services
{
    public class SqlDbService : ISqlDbService
    {
        private readonly ILogger<SqlDbService> _logger;
        private readonly string IdentifyCustomerStoredProcName = Environment.GetEnvironmentVariable("GDPRIdentifyCustomersStoredProcedureName");
        private readonly string SqlDbConnectionString = Environment.GetEnvironmentVariable("AzureSQLConnectionString");

        public SqlDbService(ILogger<SqlDbService> logger)
        {
            _logger = logger;
        }

        public async Task<List<Guid>> RetrieveCustomerIdsAsync()
        {
            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(RetrieveCustomerIdsAsync)}' has been called");

            List<Guid> customerIdList = new List<Guid>();

            using (SqlConnection connection = new SqlConnection(SqlDbConnectionString))
            {
                SqlCommand command = new SqlCommand(IdentifyCustomerStoredProcName, connection)
                {
                    CommandType = CommandType.StoredProcedure
                };

                _logger.LogInformation("Opening SQL DB connection");
                await command.Connection.OpenAsync();

                _logger.LogInformation("Executing stored proc");
                SqlDataReader reader = await command.ExecuteReaderAsync();

                while (reader.Read())
                {
                    Guid customerId = Guid.Parse(reader["ID"].ToString());
                    customerIdList.Add(customerId);
                }

                _logger.LogInformation("Closing SQL DB connection");
                await command.Connection.CloseAsync();
            }

            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(RetrieveCustomerIdsAsync)}' has finished");
            return customerIdList;
        }

        public async Task<int> PurgeDataItemsForCustomerAsync(Guid customerId)
        {
            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(PurgeDataItemsForCustomerAsync)}' has been called");

            int impactedRows = 0;

            using (SqlConnection connection = new SqlConnection(SqlDbConnectionString))
            {
                string executionQuery =
                    // master data tables
                    @"DELETE FROM [dss-actionplans] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-actions] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-addresses] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-contacts] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-diversitydetails] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-employmentprogressions] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-goals] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-interactions] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-learningprogressions] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-outcomes] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-prioritygroups] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-sessions] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-subscriptions] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-transfers] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-webchats] WHERE CustomerId=@customerId;" +

                     // history tables
                     "DELETE FROM [dss-actionplans-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-actions-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-addresses-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-contacts-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-diversitydetails-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-employmentprogressions-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-goals-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-interactions-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-learningprogressions-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-outcomes-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-prioritygroups-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-sessions-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-subscriptions-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-transfers-history] WHERE CustomerId=@customerId;" +
                     "DELETE FROM [dss-webchats-history] WHERE CustomerId=@customerId;";

                connection.Open();

                using (SqlCommand command = new SqlCommand(executionQuery, connection))
                {
                    _logger.LogInformation($"Executing the DELETE query (master and history tables)");

                    command.Parameters.AddWithValue("@customerId", customerId.ToString());
                    impactedRows = await command.ExecuteNonQueryAsync();
                }
            }

            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(PurgeDataItemsForCustomerAsync)}' has finished");
            return impactedRows;
        }

        public async Task<int> PurgeCustomerDataAsync(Guid customerId)
        {
            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(PurgeCustomerDataAsync)}' has been called");

            int impactedRows = 0;

            using (SqlConnection connection = new SqlConnection(SqlDbConnectionString))
            {
                string executionQuery =
                    // master data table
                    @"DELETE FROM [dss-customers] WHERE id=@customerId;" +

                     // history table
                     "DELETE FROM [dss-customers-history] WHERE id=@customerId;";

                connection.Open();

                using (SqlCommand command = new SqlCommand(executionQuery, connection))
                {
                    _logger.LogInformation($"Executing the DELETE query (customer table)");

                    command.Parameters.AddWithValue("@customerId", customerId.ToString());
                    impactedRows = await command.ExecuteNonQueryAsync();
                }
            }

            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(PurgeCustomerDataAsync)}' has finished");
            return impactedRows;
        }
    }
}
