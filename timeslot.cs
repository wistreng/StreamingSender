using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace EventHubSender
{
    class Timeslot
    {
        private static readonly bool SendFlag = true;
        private static readonly int TIMESLOT_DURATION = 30; 
        public static async Task TimeSlotFeeder(Dictionary<string,int> rooms)
        {
            var count = 0;
            DateTime datetime = new DateTime(2018, 2, 8, 7, 0, 0);
            DateTime enddate = new DateTime(2018, 6, 8, 0, 0, 0);
            TimeSpan ts = TimeSpan.FromMinutes(TIMESLOT_DURATION);
            List<TelemetryData> teleList = new List<TelemetryData>();
            while (SendFlag)
            {
                foreach (string roomid in rooms.Keys)
                {
                    var rnd = new Random().Next(0, rooms[roomid]);
                    var congnidata = new TelemetryData
                    {
                        Id = GenerateStringID(),
                        StartTime = datetime,
                        Duration = TIMESLOT_DURATION,
                        Result = rnd,
                        LocationId = roomid
                    };
                    teleList.Add(congnidata);
                }
                var message = JsonConvert.SerializeObject(teleList);
                Console.WriteLine($"{datetime.ToString("yyyy-mm-dd HH:mm:ss")} message {count} send");
                //await EventHubSender.SendMessagesToEventHub(message, "timeslot");
                DatabaseSender.SendMessagesToDatabase(message);
                if((datetime + ts).Hour >= 18)
                {
                    datetime = datetime.AddDays(1);
                    datetime = datetime.AddHours(-11);
                }
                if(datetime > enddate)
                {
                    break;
                }
                else
                {
                    datetime += ts;
                    count ++;
                    await Task.Delay(100);
                }
            }
        }
        private static string GenerateStringID()
        {
            long i = 1;
            foreach (byte b in Guid.NewGuid().ToByteArray())
            {
                i *= ((int)b + 1);
            }
            return string.Format("{0:x}", i - DateTime.Now.Ticks);
        }


    }
    class TelemetryData
    {
            public string Id { get; set; }
            public DateTime StartTime { get; set; }
            public int Duration { get; set; }
            public decimal Result { get; set; }
            public string LocationId { get; set; }
    }
}
