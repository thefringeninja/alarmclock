using System;
using System.Linq;
using System.Threading.Tasks;

namespace AlarmClock
{
    public class AlarmClock : Handles<TimeoutMessage>
    {
        private readonly IAlarmStorage alarms;
        private readonly Action<Message> publish;

        public AlarmClock(IAlarmStorage alarms, Action<Message> publish)
        {
            this.alarms = alarms;
            this.publish = publish;
        }

        #region Handles<TimeoutMessage> Members

        public void Handle(TimeoutMessage message)
        {
            SetAlarmAsync(message);
        }

        #endregion

        public void Start()
        {
            var startupTask = alarms.GetAll().ContinueWith(
                task =>
                {
                    var messages = task.Result.ToList();

                    alarms.InitializeAsync(messages).ContinueWith(
                        _ => messages.ForEach(SendMessageInFuture));
                });
            startupTask.Wait();
        }

        private async void SetAlarmAsync(TimeoutMessage message)
        {
            await alarms.SaveAsync(message);
            SendMessageInFuture(message);
            await alarms.DeleteAsync(message);
        }

        private void SendMessageInFuture(TimeoutMessage message)
        {
            var now = DateTime.UtcNow;
            var timeout = message.AlarmAt - now;
            if (timeout <= TimeSpan.Zero)
            {
                publish(message.Inner);
                return;
            }
            Task.Delay(timeout).ContinueWith(_ => publish(message.Inner));
        }
    }
}