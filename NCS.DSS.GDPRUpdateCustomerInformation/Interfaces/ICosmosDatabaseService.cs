namespace NCS.DSS.DataUtility.Interfaces
{
    public interface ICosmosDatabaseService
    {
        Task PurgeActionPlansForCustomerAsync(Guid customerId);
        
        Task PurgeActionsForCustomerAsync(Guid customerId);
        
        Task PurgeAddressesForCustomerAsync(Guid customerId);
        
        Task PurgeContactDetailsForCustomerAsync(Guid customerId);
    }
}
