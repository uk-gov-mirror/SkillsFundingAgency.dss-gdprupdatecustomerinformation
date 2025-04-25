namespace NCS.DSS.DataUtility.Interfaces
{
    public interface ICosmosDatabaseService
    {
        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeActionPlansForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeActionsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeAddressesForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeContactDetailsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeCustomerRecordAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeDigitalIdentitiesForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeDiversityDetailsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeEmploymentProgressionsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeGoalsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeInteractionsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeLearningProgressionsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeOutcomesForCustomerAsync(Guid customerId);

        //Task<(bool processedSuccessfully, int impactedRecordCount)> PurgePriorityGroupsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeSessionsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeSubscriptionsForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeTransfersForCustomerAsync(Guid customerId);

        Task<(bool processedSuccessfully, int impactedRecordCount)> PurgeWebchatsForCustomerAsync(Guid customerId);

        Task DeleteGenericRecordsFromContainer(string databaseName, string containerName, string field, string value, bool int_bool);
    }
}
