using FakeItEasy;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Functions;
using NCS.DSS.DataUtility.Interfaces;
using NCS.DSS.DataUtility.Models;

namespace NCS.DSS.DataUtility.Tests
{
    public class GDPRUtilityTests
    {
        private readonly ISqlDbService _mockedSqlDbService;
        private readonly ICosmosDatabaseService _mockedCosmosDbService;
        private readonly IServiceBusService _mockedServiceBusService;
        private readonly ILogger<RetrieveCustomersToBeDeleted> _mockedRetrievalFunctionLogger;
        private readonly RetrieveCustomersToBeDeleted _mockedRetrievalFunction;

        public GDPRUtilityTests()
        {
            _mockedSqlDbService = A.Fake<ISqlDbService>();
            _mockedCosmosDbService = A.Fake<ICosmosDatabaseService>();
            _mockedServiceBusService = A.Fake<IServiceBusService>();
            _mockedRetrievalFunctionLogger = A.Fake<ILogger<RetrieveCustomersToBeDeleted>>();
            _mockedRetrievalFunction = new RetrieveCustomersToBeDeleted(_mockedRetrievalFunctionLogger, _mockedSqlDbService, _mockedServiceBusService);
        }

        [Fact]
        public async Task Run_NoCustomers_NoOperationsPerformed()
        {
            // Arrange
            A.CallTo(() => _mockedSqlDbService.RetrieveCustomerIdsAsync()).Returns(Task.FromResult(new List<Guid>()));
            var timerInfo = new TimerInfo();
            Guid customerId = Guid.NewGuid();

            // Act
            await _mockedRetrievalFunction.Run(timerInfo);

            // Assert
            A.CallTo(() => _mockedServiceBusService.SendQueueMessageAsync("BODY", "QUEUE_NAME")).MustNotHaveHappened();

            A.CallTo(() => _mockedSqlDbService.PurgeDataItemsForCustomerAsync(customerId)).MustNotHaveHappened();
            A.CallTo(() => _mockedSqlDbService.PurgeCustomerDataAsync(customerId)).MustNotHaveHappened();

            A.CallTo(() => _mockedCosmosDbService.PurgeActionPlansForCustomerAsync(customerId)).MustNotHaveHappened();

            A.CallTo(_mockedRetrievalFunctionLogger).Where(call =>
                call.Method.Name == "Log"
                && call.GetArgument<LogLevel>(0) == LogLevel.Information
            ).MustHaveHappened(3, Times.Exactly);
        }

        [Fact]
        public async Task Run_SingleCustomerExists_OperationsPerformed()
        {
            // Arrange
            var customerId = new List<Guid> { Guid.NewGuid() };
            A.CallTo(() => _mockedSqlDbService.RetrieveCustomerIdsAsync()).Returns(Task.FromResult(customerId));
            var timerInfo = new TimerInfo();

            // Act
            await _mockedRetrievalFunction.Run(timerInfo);

            // Assert
            DeleteCustomerQueueMessage message = new DeleteCustomerQueueMessage
            {
                CustomerId = customerId.First()
            };

            //A.CallTo(() => _mockedServiceBusService.SendQueueMessageAsync(message, null)).MustHaveHappened(1, Times.Exactly);

            A.CallTo(_mockedRetrievalFunctionLogger).Where(call =>
                call.Method.Name == "Log"
                && call.GetArgument<LogLevel>(0) == LogLevel.Information
            ).MustHaveHappened(5, Times.Exactly);
        }
    }
}