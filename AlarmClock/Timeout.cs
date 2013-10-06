using System;
using System.Runtime.Serialization;

namespace AlarmClock
{
    [DataContract] public class Timeout<T> : TimeoutMessage where T : Message
    {
        private Timeout()
        {
        }

        public Timeout(T inner, DateTime alarmAt, Guid id)
        {
            Inner = inner;
            AlarmAt = alarmAt;
            Id = id;
        }

        #region TimeoutMessage Members

        [DataMember]
        public DateTime AlarmAt { get; private set; }

        [DataMember]
        public Guid Id { get; private set; }

        [DataMember]
        public Message Inner { get; private set; }

        #endregion
    }
}
