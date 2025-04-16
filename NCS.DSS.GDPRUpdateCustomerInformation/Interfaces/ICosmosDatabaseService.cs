namespace NCS.DSS.DataUtility.Interfaces
{
    public interface ICosmosDatabaseService
    {
        Task PurgeActionPlansForCustomerAsync(Guid customerId);
        
        Task PurgeActionsForCustomerAsync(Guid customerId);
        
        Task PurgeAddressesForCustomerAsync(Guid customerId);
        
        Task PurgeContactDetailsForCustomerAsync(Guid customerId);

        Task PurgeDigitalIdentitiesForCustomerAsync(Guid customerId);

        Task PurgeDiversityDetailsForCustomerAsync(Guid customerId);

        Task PurgeEmploymentProgressionsForCustomerAsync(Guid customerId);

        Task PurgeGoalsForCustomerAsync(Guid customerId);
    }
}
