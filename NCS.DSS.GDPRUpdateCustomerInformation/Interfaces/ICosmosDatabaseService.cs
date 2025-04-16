namespace NCS.DSS.DataUtility.Interfaces
{
    public interface ICosmosDatabaseService
    {
        Task<int> PurgeActionPlansForCustomerAsync(Guid customerId);
        
        Task<int> PurgeActionsForCustomerAsync(Guid customerId);
        
        Task<int> PurgeAddressesForCustomerAsync(Guid customerId);
        
        Task<int> PurgeContactDetailsForCustomerAsync(Guid customerId);

        Task<int> PurgeDigitalIdentitiesForCustomerAsync(Guid customerId);

        Task<int> PurgeDiversityDetailsForCustomerAsync(Guid customerId);

        Task<int> PurgeEmploymentProgressionsForCustomerAsync(Guid customerId);

        Task<int> PurgeGoalsForCustomerAsync(Guid customerId);

        Task<int> PurgeLearningProgressionsForCustomerAsync(Guid customerId);

        Task<int> PurgeOutcomesForCustomerAsync(Guid customerId);

        Task<int> PurgeSessionsForCustomerAsync(Guid customerId);
    }
}
