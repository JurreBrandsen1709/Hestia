using System;

namespace Dyconit.Overlord;

public class DyconitPerformanceLogger
{
    private DateTime? lastConsumedTime;
    private int stalenessBound;

    public DyconitPerformanceLogger(int stalenessBound)
    {
        this.stalenessBound = stalenessBound;
    }

    public void LogConsumedMessage(DateTime consumedTime)
    {
        if (lastConsumedTime.HasValue)
        {
            var timeDifference = consumedTime - lastConsumedTime.Value;
            Console.WriteLine($"Time difference from last consumed message: {timeDifference.TotalMilliseconds}");

            if (timeDifference.TotalMilliseconds  > stalenessBound)
            {
                Console.WriteLine("Time is outside the staleness bound.");
            }
            else
            {
                Console.WriteLine("Time is within the staleness bound.");
            }
        }

        lastConsumedTime = consumedTime;
        Console.WriteLine($"Last consumed message time: {lastConsumedTime}");
    }
}