namespace NCS.DSS.DataUtility.Interfaces
{
    public interface IServiceBusService
    {
        Task<bool> SendQueueMessageAsync<T>(T messageBody, string queueName);
    }
}
