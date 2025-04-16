namespace NCS.DSS.DataUtility.Interfaces
{
    public interface ICosmosDatabaseService
    {
        Task PurgeActionPlansForCustomerAsync(Guid customerId);
    }
}
