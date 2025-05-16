namespace NCS.DSS.DataUtility.Interfaces
{
    public interface ISqlDbService
    {
        Task<List<Guid>> RetrieveCustomerIdsAsync();

        Task<int> PurgeDataItemsForCustomerAsync(Guid customerId);

        Task<int> PurgeCustomerDataAsync(Guid customerId);
    }
}
