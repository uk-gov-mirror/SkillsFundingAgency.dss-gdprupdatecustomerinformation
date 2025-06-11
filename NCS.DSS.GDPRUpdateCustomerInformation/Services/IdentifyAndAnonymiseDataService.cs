using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;

namespace NCS.DSS.DataUtility.Services
{
    public class IdentifyAndAnonymiseDataService : IIdentifyAndAnonymiseDataService
    {
        private readonly string _GDPRUpdateCustomersStoredProcedureName = Environment.GetEnvironmentVariable("GDPRUpdateCustomersStoredProcedureName");
        private readonly string _GDPRIdentifyCustomersStoredProcedureName = Environment.GetEnvironmentVariable("GDPRIdentifyCustomersStoredProcedureName");
        private readonly string _sqlConnectionString = Environment.GetEnvironmentVariable("AzureSQLConnectionString");

        private readonly ILogger<IIdentifyAndAnonymiseDataService> _logger;
        private readonly ICosmosDBService _cosmosDBService;
        private readonly SqlConnection _sqlConnection;

        public IdentifyAndAnonymiseDataService(ICosmosDBService cosmosDBService, ILogger<IIdentifyAndAnonymiseDataService> logger)
        {
            _cosmosDBService = cosmosDBService;
            _logger = logger;
            _sqlConnection = new SqlConnection(_sqlConnectionString);
        }

        public async Task AnonymiseData()
        {
            await ExecuteUpdateStoredProcedureAsync();
        }

        public async Task DeleteCustomersFromCosmos(List<Guid> customerIds)
        {
            if (customerIds != null)
            {
                foreach (Guid customerId in customerIds)
                {
                    await _cosmosDBService.DeleteRecordsForCustomer(customerId);
                }
            }
        }

        public async Task<List<Guid>> ReturnCustomerIds()
        {
            return await ExecuteIdentifyStoredProcedureAsync();
        }

        private async Task<List<Guid>> ExecuteIdentifyStoredProcedureAsync()
        {
            await using var command = new SqlCommand(_GDPRIdentifyCustomersStoredProcedureName, _sqlConnection);
            command.CommandType = CommandType.StoredProcedure;

            try
            {
                _logger.LogInformation("Opening the database connection");
                await _sqlConnection.OpenAsync();
                _logger.LogInformation("Attempting to execute the stored procedure: {StoredProcName}", _GDPRIdentifyCustomersStoredProcedureName);
                SqlDataReader reader = await command.ExecuteReaderAsync();

                List<Guid> idList = new List<Guid>();

                while (reader.Read())
                {
                    var id = Guid.Parse(reader["ID"].ToString());
                    idList.Add(id);
                }

                _logger.LogInformation("Finished executing the stored procedure: {StoredProcName}", _GDPRIdentifyCustomersStoredProcedureName);
                return idList;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to execute the stored procedure: {StoredProcName}. Error: {ErrorMessage}", _GDPRIdentifyCustomersStoredProcedureName, ex.Message);
                throw;
            }
            finally
            {
                _logger.LogInformation("Closing the database connection");
                await _sqlConnection.CloseAsync();
            }
        }

        private async Task ExecuteUpdateStoredProcedureAsync()
        {
            await using var command = new SqlCommand(_GDPRUpdateCustomersStoredProcedureName, _sqlConnection);
            command.CommandType = CommandType.StoredProcedure;

            try
            {
                _logger.LogInformation("Opening the database connection");
                await _sqlConnection.OpenAsync();
                _logger.LogInformation("Attempting to execute the stored procedure: {StoredProcName}", _GDPRUpdateCustomersStoredProcedureName);
                await command.ExecuteNonQueryAsync();
                _logger.LogInformation("Finished executing the stored procedure: {StoredProcName}", _GDPRUpdateCustomersStoredProcedureName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to execute the stored procedure: {StoredProcName}. Error: {ErrorMessage}", _GDPRUpdateCustomersStoredProcedureName, ex.Message);
                throw;
            }
            finally
            {
                _logger.LogInformation("Closing the database connection");
                await _sqlConnection.CloseAsync();
            }
        }
    }
}
