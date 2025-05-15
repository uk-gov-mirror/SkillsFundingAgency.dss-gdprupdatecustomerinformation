using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Interfaces;
using Newtonsoft.Json;

namespace NCS.DSS.DataUtility.Services
{
    public class ServiceBusService : IServiceBusService
    {
        private readonly ILogger<ServiceBusService> _logger;
        private readonly ServiceBusClient _serviceBusClient;
        private readonly Dictionary<string, ServiceBusSender> _senders = new Dictionary<string, ServiceBusSender>();

        public ServiceBusService(ILogger<ServiceBusService> logger, ServiceBusClient serviceBusClient)
        {
            _logger = logger;
            _serviceBusClient = serviceBusClient;
        }

        public async Task<bool> SendQueueMessageAsync<T>(T messageBody, string queueName)
        {
            _logger.LogInformation("ServiceBusService method 'SendQueueMessageAsync' has been called");
            _logger.LogInformation($"Attempting to send message onto queue '{queueName}'");

            ServiceBusSender serviceBusSender = GetServiceBusSender(queueName);

            string jsonSerialized = JsonConvert.SerializeObject(messageBody);
            byte[] jsonAsByteArray = System.Text.Encoding.UTF8.GetBytes(jsonSerialized);

            ServiceBusMessage message = new ServiceBusMessage(jsonAsByteArray)
            {
                ContentType = "application/json"
            };

            try
            {
                await serviceBusSender.SendMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Unable to send message to queue. Exception: {ex.Message}");
                return false;
            }

            _logger.LogInformation($"Message was sent successfully");

            return true;
        }

        private ServiceBusSender GetServiceBusSender(string queueName)
        {
            if (_senders.TryGetValue(queueName, out ServiceBusSender storedSender))
            {
                return storedSender;
            }

            _senders[queueName] = _serviceBusClient.CreateSender(queueName);
            return _senders[queueName];
        }
    }
}
