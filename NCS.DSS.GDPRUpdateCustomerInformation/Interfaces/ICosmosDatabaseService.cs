namespace NCS.DSS.DataUtility.Interfaces
{
    public interface ICosmosDatabaseService
    {
        Task<bool> PurgeActionPlansForCustomerAsync(Guid customerId);
    }
}
