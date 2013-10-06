using System.Runtime.Serialization;

namespace AlarmClock.Tests
{
    [DataContract] public class SomeEvent : Message
    {
        [DataMember] public readonly int Number;

        public SomeEvent(int number)
        {
            Number = number;
        }
    }
}