using System.Collections.Generic;
using System.Threading.Tasks;

namespace AlarmClock
{
    public interface IAlarmStorage
    {
        Task<IEnumerable<TimeoutMessage>> GetAll();
        Task SaveAsync(TimeoutMessage message);
        Task DeleteAsync(TimeoutMessage message);
        Task InitializeAsync(IEnumerable<TimeoutMessage> messages);
    }
}