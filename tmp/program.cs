// using System;
// using System.Collections.Generic;
// using System.IO;
// using System.Text.Json;

// class Program
// {
//     private const string FileName = "numbers.json";
//     private static Random random = new Random();
//     private static Dictionary<int, int> numbers = new Dictionary<int, int>();

//     static void Main(string[] args)
//     {
//         if (File.Exists(FileName))
//         {
//             numbers = LoadDictionary();
//         }
//         else
//         {
//             PopulateDictionary();
//             SaveDictionary();
//         }

//         while (true)
//         {
//             Console.Write("Enter a key: ");
//             string input = Console.ReadLine();
//             if (int.TryParse(input, out int key) && numbers.ContainsKey(key))
//             {
//                 Console.WriteLine($"Value at key {key}: {numbers[key]}");
//             }
//             else
//             {
//                 Console.WriteLine("Invalid key. Please try again.");
//             }
//         }
//     }

//     private static void PopulateDictionary()
//     {
//         for (int i = 0; i < 2000; i++)
//         {
//             numbers[i] = random.Next(200, 601); // Upper bound is exclusive
//         }
//     }

//     private static void SaveDictionary()
//     {
//         string jsonString = JsonSerializer.Serialize(numbers);
//         File.WriteAllText(FileName, jsonString);
//     }

//     private static Dictionary<int, int> LoadDictionary()
//     {
//         string jsonString = File.ReadAllText(FileName);
//         return JsonSerializer.Deserialize<Dictionary<int, int>>(jsonString);
//     }
// }
