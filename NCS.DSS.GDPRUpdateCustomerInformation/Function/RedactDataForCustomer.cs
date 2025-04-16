using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Models;
using Newtonsoft.Json;
using System.Text;

namespace NCS.DSS.DataUtility.Function
{
    public class RedactDataForCustomer
    {
        private readonly ILogger<RedactDataForCustomer> _logger;

        public RedactDataForCustomer(ILogger<RedactDataForCustomer> logger)
        {
            _logger = logger;
        }

        [Function(nameof(RedactDataForCustomer))]
        public async Task<IActionResult> Run([ServiceBusTrigger("%RedactionQueueName%")] ServiceBusReceivedMessage message, ServiceBusMessageActions messageActions)
        {
            _logger.LogInformation($"Function '{nameof(RedactDataForCustomer)}' has been invoked");

            // convert queue message into usage object
            var bodyText = Encoding.UTF8.GetString(message.Body);
            RedactionQueueMessage queueBody = JsonConvert.DeserializeObject<RedactionQueueMessage>(bodyText);

            _logger.LogInformation($"Customer with ID '{queueBody.CustomerId.ToString()}' will now be processed");

            

            //_logger.LogInformation("Message ID: {id}", message.MessageId);
            //_logger.LogInformation("Message Body: {body}", message.Body); // RedactionQueueMessage
            //_logger.LogInformation("Message Content-Type: {contentType}", message.ContentType);

            // Complete the message
            await messageActions.CompleteMessageAsync(message);
            return new OkResult();
        }
    }
}
