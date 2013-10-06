using System;
using System.Linq;
using System.Threading;
using AlarmClock.Persistence;
using Newtonsoft.Json;
using NUnit.Framework;

namespace AlarmClock.Tests
{
    [TestFixture] public class EventStoreAlarmPersistenceAcceptanceTests : EventStoreIntegrationTests
    {
        #region Setup/Teardown

        [SetUp]
        public void Setup()
        {
            wait = new ManualResetEvent(false);
            streamId = "alarm-clock-" + Guid.NewGuid().ToString("n");
            alarmStorage = new EventStoreAlarmStorage(
                EventStore.Connection,
                new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.None
                },
                streamId);

            sut = new AlarmClock(
                alarmStorage,
                _ => wait.Set());
        }

        #endregion

        private ManualResetEvent wait;

        private AlarmClock sut;
        private string streamId;
        private EventStoreAlarmStorage alarmStorage;

        [Test]
        public void recover_from_shutdown()
        {
            alarmStorage.InitializeAsync(
                new[] {new Timeout<Message>(new SomeEvent(1), DateTime.UtcNow.AddSeconds(2), Guid.NewGuid())})
                        .Wait();

            sut.Start();

            Assert.IsTrue(wait.WaitOne(TimeSpan.FromSeconds(5)));
        }

        [Test]
        public void single_timeout()
        {
            sut.Start();

            sut.Handle(new Timeout<Message>(new SomeEvent(1), DateTime.UtcNow.AddSeconds(1), Guid.NewGuid()));

            Assert.IsTrue(wait.WaitOne(TimeSpan.FromSeconds(5)));
        }
        [Test]
        public void only_initializes_since_last_time_started()
        {
            alarmStorage.InitializeAsync(
                new[] { new Timeout<Message>(new SomeEvent(1), DateTime.UtcNow.AddSeconds(1), Guid.NewGuid()) })
                .Wait();

            alarmStorage.InitializeAsync(
                new[] { new Timeout<Message>(new SomeEvent(1), DateTime.UtcNow.AddSeconds(3), Guid.NewGuid()) })
                .Wait();

            sut.Start();

            Assert.IsTrue(wait.WaitOne(TimeSpan.FromSeconds(5)));
            wait.Reset();
            Assert.IsFalse(wait.WaitOne(TimeSpan.FromSeconds(5)));
        }

        [Test]
        public void fired_alarms_are_not_sent_after_recovering_from_shutdown()
        {
            var message = new Timeout<Message>(new SomeEvent(1), DateTime.UtcNow.AddSeconds(1), Guid.NewGuid());
            
            alarmStorage.SaveAsync(message).Wait();
            
            alarmStorage.DeleteAsync(message).Wait();

            sut.Start();

            Assert.IsFalse(wait.WaitOne(TimeSpan.FromSeconds(5)));
        }

        [Test]
        public void can_store_and_retrieve_lots_of_alarms()
        {
            alarmStorage.InitializeAsync(
                Enumerable.Repeat<TimeoutMessage>(
                    new Timeout<Message>(new SomeEvent(1), DateTime.UtcNow.AddSeconds(1), Guid.NewGuid()),
                    2049)).Wait();

            Assert.AreEqual(2049, alarmStorage.GetAll().Result.Count());
        }

        [Test]
        public void alarms_in_past_are_sent_immediately()
        {
            alarmStorage.InitializeAsync(
                new[] { new Timeout<Message>(new SomeEvent(1), DateTime.UtcNow.AddDays(-1), Guid.NewGuid()) })
                .Wait();
            
            sut.Start();

            Assert.IsTrue(wait.WaitOne(TimeSpan.FromSeconds(5)));
        }

    }
}
