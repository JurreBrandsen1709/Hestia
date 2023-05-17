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
            var overlord = new DyconitOverlord();

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
                else
                {
                    Console.WriteLine($"Unknown command: {input}");
                }
            }
        }
    }
}
