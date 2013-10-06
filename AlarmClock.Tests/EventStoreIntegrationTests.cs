using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;

namespace AlarmClock.Tests
{
    public abstract class EventStoreIntegrationTests
    {
        protected EmbeddedEventStore EventStore;
        private string folder;

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            folder = Path.Combine(
                Path.GetTempPath(),
                DateTime.UtcNow.ToString("yyyy.MM.dd.HH.mm.ss"));
            EventStore = new EmbeddedEventStore(folder);
            EventStore.Start();

            Task.Delay(1000).Wait();
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            EventStore.Stop();
            Directory.Delete(folder, true);
        }
    }
}
