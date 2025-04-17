using FakeItEasy;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace NCS.DSS.DataUtility.Tests
{
    public class GDPRUtilityTests
    {
        /*private readonly IIdentifyAndAnonymiseDataService _fakeDataService;
        private readonly ILogger<Function.GDPRUtility> _fakeLogger;
        private readonly Function.GDPRUtility _function;

        public GDPRUtilityTests()
        {
            _fakeDataService = A.Fake<IIdentifyAndAnonymiseDataService>();
            _fakeLogger = A.Fake<ILogger<Function.GDPRUtility>>();
            _function = new Function.GDPRUtility(_fakeDataService, _fakeLogger);
        }

        [Fact]
        public async Task Run_NoCustomers_NoOperationsPerformed()
        {
            // Arrange
            A.CallTo(() => _fakeDataService.ReturnCustomerIds()).Returns(Task.FromResult(new List<Guid>()));
            var timerInfo = new TimerInfo();

            // Act
            await _function.RunAsync(timerInfo);

            // Assert
            A.CallTo(() => _fakeDataService.AnonymiseData()).MustNotHaveHappened();
            A.CallTo(() => _fakeDataService.DeleteCustomersFromCosmos(A<List<Guid>>.Ignored)).MustNotHaveHappened();
        }

        [Fact]
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