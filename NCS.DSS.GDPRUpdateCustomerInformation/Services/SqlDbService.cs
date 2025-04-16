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
            _logger.LogInformation("SqlDbService function 'RetrieveCustomerIdsAsync' has been called");

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
                await reader.CloseAsync();
            }

            _logger.LogInformation("SqlDbService function 'RetrieveCustomerIdsAsync' has finished");
            return customerIdList;
        }
    }
}
