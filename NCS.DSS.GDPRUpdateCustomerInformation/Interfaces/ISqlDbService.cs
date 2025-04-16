namespace NCS.DSS.DataUtility.Interfaces
{
    public interface ISqlDbService
    {
        Task<List<Guid>> RetrieveCustomerIdsAsync();
    }
}
