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

    public void BoundStaleness(DateTime consumedTime)
    {
        // send message to dyconit overlord with consumedTime
        // The overlord will check if other affected nodes belonging to the same Collection of dyconit has synchronized its updates
        // with the rest of the nodes within the set stalenessBound. if not, it will direct that node to synchronize its updates with
        // this node.

    }
}