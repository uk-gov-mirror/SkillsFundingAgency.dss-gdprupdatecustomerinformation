using FakeItEasy;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NCS.DSS.DataUtility.Functions;
using NCS.DSS.DataUtility.Interfaces;

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
            A.CallTo(() => _mockedSqlDbService.PurgeDataItemsForCustomerAsync(customerId)).MustNotHaveHappened();
            A.CallTo(() => _mockedSqlDbService.PurgeCustomerDataAsync(customerId)).MustNotHaveHappened();

            A.CallTo(() => _mockedCosmosDbService.PurgeActionPlansForCustomerAsync(customerId)).MustNotHaveHappened();

            A.CallTo(_mockedRetrievalFunctionLogger).Where(call => 
                call.Method.Name == "Log" 
                && call.GetArgument<LogLevel>(0) == LogLevel.Information
            ).MustHaveHappened(3, Times.Exactly);
        }

        /*[Fact]
        public async Task Run_CustomersExist_OperationsPerformed()
        {
            // Arrange
            var customerIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() };
            A.CallTo(() => _fakeDataService.ReturnCustomerIds()).Returns(Task.FromResult(customerIds));
            var timerInfo = new TimerInfo();

            // Act
            await _function.RunAsync(timerInfo);

            // Assert
            A.CallTo(() => _fakeDataService.AnonymiseData()).MustHaveHappenedOnceExactly();
            A.CallTo(() => _fakeDataService.DeleteCustomersFromCosmos(customerIds)).MustHaveHappenedOnceExactly();
        }*/
    }
}