using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using System.Data;

namespace NCS.DSS.DataUtility.Services
{
    public class SqlDbService : ISqlDbService
    {
        private readonly ILogger<SqlDbService> _logger;
        private readonly string _identifyCustomerStoredProcName = Environment.GetEnvironmentVariable("GDPRIdentifyCustomersStoredProcedureName");
        private readonly string _sqlDbConnectionString = Environment.GetEnvironmentVariable("AzureSQLConnectionString");

        public SqlDbService(ILogger<SqlDbService> logger)
        {
            _logger = logger;
        }

        public async Task<List<Guid>> RetrieveCustomerIdsAsync()
        {
            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(RetrieveCustomerIdsAsync)}' has been called");

            List<Guid> customerIdList = new List<Guid>();

            using (SqlConnection connection = new SqlConnection(_sqlDbConnectionString))
            {
                SqlCommand command = new SqlCommand(_identifyCustomerStoredProcName, connection)
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

            using (SqlConnection connection = new SqlConnection(_sqlDbConnectionString))
            {
                string executionQuery =
                    // master data tables
                    @"DELETE FROM [dss-actionplans] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-actions] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-addresses] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-contacts] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-diversitydetails] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-employmentprogressions] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-goals] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-learningprogressions] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-outcomes] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-prioritygroups] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-sessions] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-subscriptions] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-transfers] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-webchats] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +

                     // history tables
                     "DELETE FROM [dss-actionplans-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-actions-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-addresses-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-contacts-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-diversitydetails-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-employmentprogressions-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-goals-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-learningprogressions-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-outcomes-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-prioritygroups-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-sessions-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-subscriptions-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-transfers-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-webchats-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);";

                connection.Open();

                using (SqlCommand command = new SqlCommand(executionQuery, connection))
                {
                    _logger.LogInformation($"Executing the DELETE query (master and history tables)");

                    command.Parameters.Add("@customerId", SqlDbType.UniqueIdentifier).Value = customerId;
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

            using (SqlConnection connection = new SqlConnection(_sqlDbConnectionString))
            {
                string executionQuery =
                    // master data table
                    @"DELETE FROM [dss-customers] WHERE id=@customerId OPTION (MAXDOP 1);" +
                    "DELETE FROM [dss-interactions] WHERE CustomerId=@customerId OPTION (MAXDOP 1);" +

                     // history table
                     "DELETE FROM [dss-customers-history] WHERE id=@customerId OPTION (MAXDOP 1);" +
                     "DELETE FROM [dss-interactions-history] WHERE CustomerId=@customerId OPTION (MAXDOP 1);";

                connection.Open();

                using (SqlCommand command = new SqlCommand(executionQuery, connection))
                {
                    _logger.LogInformation($"Executing the DELETE query (customer table)");

                    command.Parameters.Add("@customerId", SqlDbType.UniqueIdentifier).Value = customerId;
                    impactedRows = await command.ExecuteNonQueryAsync();
                }
            }

            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(PurgeCustomerDataAsync)}' has finished");
            return impactedRows;
        }

        public async Task PurgeRecordDataAsync(Guid recordId, string tableName)
        {
            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(PurgeRecordDataAsync)}' has been called");

            using (SqlConnection connection = new SqlConnection(_sqlDbConnectionString))
            {
                string executionQuery =
                    // master data table
                    @"DELETE FROM [dss-" + @tableName + "] WHERE id=@recordId;" +

                     // history table
                     "DELETE FROM [dss-" + @tableName + "-history] WHERE id=@recordId;";

                connection.Open();

                using (SqlCommand command = new SqlCommand(executionQuery, connection))
                {
                    _logger.LogInformation($"Executing the DELETE query ({tableName} table)");

                    command.Parameters.AddWithValue("@recordId", recordId.ToString());
                    await command.ExecuteNonQueryAsync();
                }
            }

            _logger.LogInformation($"{nameof(SqlDbService)} method '{nameof(PurgeRecordDataAsync)}' has finished");
        }
    }
}
