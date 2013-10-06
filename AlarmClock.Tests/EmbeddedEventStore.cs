using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Services.Monitoring;
using EventStore.Core.Settings;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.Util;

namespace AlarmClock.Tests
{
    public class EmbeddedEventStore
    {
        private static readonly IPEndPoint TcpEndPoint = new IPEndPoint(IPAddress.Loopback, 1113);
        private static readonly IPEndPoint HttpEndPoint = new IPEndPoint(IPAddress.Loopback, 2113);

        private readonly string storagePath;
        private IEventStoreConnection connection;
        private SingleVNode node;

        public EmbeddedEventStore(string storagePath)
        {
            this.storagePath = storagePath;
        }

        public IEventStoreConnection Connection
        {
            get { return connection; }
        }

        public void Start()
        {
            var db = CreateTFChunkDb(storagePath);
            var settings = CreateSingleVNodeSettings();
            node = new SingleVNode(db, settings, false, 0xf4240, new ISubsystem[0]);
            var waitHandle = new ManualResetEvent(false);
            node.MainBus.Subscribe(new AdHocHandler<SystemMessage.BecomeMaster>(m => waitHandle.Set()));
            node.Start();
            waitHandle.WaitOne();

            connection =
                EventStoreConnection.Create(
                    ConnectionSettings.Create().UseConsoleLogger().EnableVerboseLogging()
                                      .SetDefaultUserCredentials(new UserCredentials("admin", "changeit")),
                    TcpEndPoint);

            connection.Connect();
        }

        public void Stop()
        {
            if (connection != null)
            {
                connection.Close();
            }
            if (node != null)
            {
                var wait = new ManualResetEvent(false);
                node.MainBus.Subscribe(
                    new AdHocHandler<SystemMessage.BecomeShutdown>(
                        _ => wait.Set()));
                node.Stop(false);
                wait.WaitOne();
            }
        }

        private static TFChunkDb CreateTFChunkDb(string storagePath)
        {
            var dbPath = Path.Combine(storagePath, DateTime.Now.Ticks.ToString(CultureInfo.InvariantCulture));
            if (!Directory.Exists(dbPath))
            {
                Directory.CreateDirectory(dbPath);
            }
            var writerCheckFilename = Path.Combine(dbPath, Checkpoint.Writer + ".chk");
            var chaserCheckFilename = Path.Combine(dbPath, Checkpoint.Chaser + ".chk");
            var epochCheckFilename = Path.Combine(dbPath, Checkpoint.Epoch + ".chk");
            var truncateCheckFilename = Path.Combine(dbPath, Checkpoint.Truncate + ".chk");
            //Not mono friendly at this point.
            var writerChk = new MemoryMappedFileCheckpoint(writerCheckFilename, "writer", true);
            var chaserChk = new MemoryMappedFileCheckpoint(chaserCheckFilename, "chaser", true);
            var epochChk = new MemoryMappedFileCheckpoint(
                epochCheckFilename,
                Checkpoint.Epoch,
                true,
                initValue: -1);
            var truncateChk = new MemoryMappedFileCheckpoint(
                truncateCheckFilename,
                Checkpoint.Truncate,
                true,
                initValue: -1);
            const int cachedChunks = 100;
            return new TFChunkDb(
                new TFChunkDbConfig(
                    dbPath,
                    new VersionedPatternFileNamingStrategy(dbPath, "chunk-"),
                    0x10000000,
                    cachedChunks*0x10000100L,
                    writerChk,
                    chaserChk,
                    epochChk,
                    truncateChk));
        }

        private static SingleVNodeSettings CreateSingleVNodeSettings()
        {
            var settings = new SingleVNodeSettings(
                TcpEndPoint,
                null,
                new IPEndPoint(IPAddress.None, 0),
                new string[0],
                false,
                null,
                Opts.WorkerThreadsDefault,
                Opts.MinFlushDelayMsDefault,
                TimeSpan.FromMilliseconds(Opts.PrepareTimeoutMsDefault),
                TimeSpan.FromMilliseconds(Opts.CommitTimeoutMsDefault),
                TimeSpan.FromMilliseconds(Opts.StatsPeriodDefault),
                StatsStorage.None);
            return settings;
        }
    }
}
