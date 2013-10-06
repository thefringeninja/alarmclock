using System;

namespace AlarmClock
{
    public interface TimeoutMessage : Message
    {
        DateTime AlarmAt { get; }
        Guid Id { get; }
        Message Inner { get; }
    }
}