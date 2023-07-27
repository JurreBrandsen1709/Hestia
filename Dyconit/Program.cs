// todo parse policies and share with overlord?

using System;
using Dyconit.Overlord;

namespace Dyconit
{
    class Program
    {
        static void Main(string[] args)
        {
            // Create an instance of DyconitOverlord
            var dyconitOverlord = new DyconitOverlord();
            Console.WriteLine("- Dyconit overlord started.");
            dyconitOverlord.ParsePolicies();
            Console.WriteLine("- Policies parsed.");
            dyconitOverlord.StartListening();
            dyconitOverlord.SendHeartbeatAsync();
            dyconitOverlord.KeepTrackOfNodesAsync();


            Console.WriteLine("Press Ctrl+C to stop...");
            // Listen for console input
            while (true)
            {
                var input = Console.ReadLine();
                if (string.IsNullOrEmpty(input))
                {
                    continue;
                }
                else if (input.ToLower() == "exit")
                {
                    break;
                }
                else if (Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.C && (ConsoleModifiers.Control & ConsoleModifiers.Control) != 0)
                {
                    dyconitOverlord.StopListening();
                    break;
                }
                else
                {
                    Console.WriteLine($"Unknown command: {input}");
                }
            }
        }
    }
}
