using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace AlarmClock.Persistence
{
    public class EventStoreAlarmStorage : IAlarmStorage
    {
        public const string AlarmClockStarted = "alarm-clock-started";
        public const string AlarmFired = "alarm-fired";
        public const string AlarmSet = "alarm-set";
        public const string AlarmClockStarting = "alarm-clock-starting";

        private const int PageSize = 512;
        private readonly IEventStoreConnection eventStore;
        private readonly JsonSerializerSettings serializerSettings;
        private readonly string streamId;

        public EventStoreAlarmStorage(
            IEventStoreConnection eventStore, JsonSerializerSettings serializerSettings, String streamId = null)
        {
            this.eventStore = eventStore;
            this.serializerSettings = serializerSettings;
            this.streamId = streamId ?? "alarm-clock";
        }

        #region IAlarmStorage Members

        public async Task<IEnumerable<TimeoutMessage>> GetAll()
        {
            var unfiredAlarms = new List<TimeoutMessage>();
            var alreadyFired = new List<Guid>();

            var slice = await eventStore.ReadStreamEventsBackwardAsync(streamId, StreamPosition.End, PageSize, false);

            if (slice.Events.Length == 0)
                return Enumerable.Empty<TimeoutMessage>();

            var readLock = await eventStore.StartTransactionAsync(streamId, slice.Events[0].OriginalEventNumber);
            await readLock.WriteAsync(CreateAlarmClockStarting());

            var allTimeoutMessages = await ReadTimeoutMessages(slice, unfiredAlarms, alreadyFired);

            await readLock.CommitAsync();

            return allTimeoutMessages.AsReadOnly();
        }

        private async Task<List<TimeoutMessage>> ReadTimeoutMessages(StreamEventsSlice slice, List<TimeoutMessage> unfiredAlarms, List<Guid> alreadyFired)
        {
            bool isEndOfStream = false;
            do
            {
                isEndOfStream = slice.IsEndOfStream;
                foreach (var resolvedEvent in slice.Events)
                {
                    // this means we are at the point when the last alarm clock started and we're done.
                    var originalEvent = resolvedEvent.OriginalEvent;
                    if (originalEvent.EventType == AlarmClockStarted)
                        return unfiredAlarms;

                    if (originalEvent.EventType == AlarmClockStarting)
                    {
                        throw new InvalidOperationException("Another instance of the alarm clock is already using stream " + streamId + ".");
                    }

                    if (originalEvent.EventType == AlarmFired)
                    {
                        var fired = JsonConvert.DeserializeAnonymousType(
                            Encoding.UTF8.GetString(originalEvent.Data),
                            new
                            {
                                Id = Guid.Empty
                            },
                            serializerSettings);

                        alreadyFired.Add(fired.Id);
                    }
                    else if (originalEvent.EventType == AlarmSet)
                    {
                        var headers = JsonConvert.DeserializeAnonymousType(
                            Encoding.UTF8.GetString(originalEvent.Metadata),
                            new
                            {
                                ClrType = String.Empty
                            },
                            serializerSettings);

                        var type = Type.GetType(headers.ClrType);

                        var timeout = (TimeoutMessage) JsonConvert.DeserializeObject(
                            Encoding.UTF8.GetString(originalEvent.Data),
                            typeof (Timeout<>).MakeGenericType(type),
                            serializerSettings);

                        if (false == alreadyFired.Contains(timeout.Id))
                        {
                            unfiredAlarms.Add(timeout);
                        }
                    }
                }

                slice = await eventStore.ReadStreamEventsBackwardAsync(streamId, slice.NextEventNumber, PageSize, false);
            } while (false == isEndOfStream);

            return unfiredAlarms;
        }

        public async Task SaveAsync(TimeoutMessage message)
        {
            var eventData = CreateAlarmSet(message);

            await eventStore.AppendToStreamAsync(streamId, ExpectedVersion.Any, eventData);
        }

        public async Task DeleteAsync(TimeoutMessage message)
        {
            var eventData = CreateAlarmFired(message);

            await eventStore.AppendToStreamAsync(streamId, ExpectedVersion.Any, eventData);
        }

        public async Task InitializeAsync(IEnumerable<TimeoutMessage> messages)
        {
            var eventDatum = new List<EventData>
            {
                CreateAlarmClockStarted()
            };

            eventDatum.AddRange(messages.Select(CreateAlarmSet));

            if (eventDatum.Count < PageSize)
            {
                await eventStore.AppendToStreamAsync(streamId, ExpectedVersion.Any, eventDatum);
            }
            else
            {
                var transaction = await eventStore.StartTransactionAsync(streamId, ExpectedVersion.Any);

                for (var i = 0; i < eventDatum.Count; i += PageSize)
                {
                    await transaction.WriteAsync(eventDatum.Skip(i).Take(PageSize));
                }

                await transaction.CommitAsync();
            }
        }

        #endregion

        private EventData CreateAlarmClockStarted()
        {
            return new EventData(Guid.NewGuid(), AlarmClockStarted, false, null, null);
        }
        private EventData CreateAlarmClockStarting()
        {
            return new EventData(Guid.NewGuid(), AlarmClockStarting, false, null, null);
        }


        private EventData CreateAlarmFired(TimeoutMessage timeout)
        {
            var body = JsonConvert.SerializeObject(
                new
                {
                    timeout.Id
                },
                serializerSettings);

            var data = Encoding.UTF8.GetBytes(body);

            return new EventData(Guid.NewGuid(), AlarmFired, true, data, null);
        }

        private EventData CreateAlarmSet(TimeoutMessage timeout)
        {
            var body = JsonConvert.SerializeObject(timeout, serializerSettings);
            var data = Encoding.UTF8.GetBytes(body);

            var headers = JsonConvert.SerializeObject(
                new
                {
                    ClrType = timeout.Inner.GetType().ToPartiallyQualifiedName()
                },
                serializerSettings);
            var metadata = Encoding.UTF8.GetBytes(headers);

            return new EventData(Guid.NewGuid(), AlarmSet, true, data, metadata);
        }
    }
}
