using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using CosmosDbDemo.Demos;

namespace CosmosDbDemo
{
    public static class Program
    {
        private static IDictionary<string, Func<Task>> _demoMethods;

        private static async Task Main()
        {
            _demoMethods = new Dictionary<string, Func<Task>>
            {
                {"DB", DatabaseDemo.Run},
                {"CO", CollectionsDemo.Run},
                {"DO", DocumentsDemo.Run},
                {"IX", IndexingDemo.Run},
                {"UP", UsersAndPermissionsDemo.Run},
                {"SP", StoredProceduresDemo.Run},
                {"TR", TriggersDemo.Run},
                {"UF", UserDefinedFunctionsDemo.Run},
                { "C", Cleanup.Run}
            };

            await ReadEvalPrintLoop();
        }

        private static async Task ReadEvalPrintLoop()
        {
            while (true)
            {
                ShowMenu();

                Console.Write("Selection: ");
                string input = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(input))
                {
                    continue;
                }

                string demoId = input.ToUpper().Trim();

                if (_demoMethods.Keys.Contains(demoId))
                {
                    Func<Task> demoMethod = _demoMethods[demoId];
                    await RunDemo(demoMethod);

                    Console.WriteLine();
                    Console.Write("Done. Press any key to continue.");
                    Console.ReadKey(true);
                    Console.Clear();
                }
                else if (demoId == "Q")
                {
                    break;
                }
                else
                {
                    Console.WriteLine("Invalid input. Try again.");
                }
            }
        }

        private static void ShowMenu()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine("Cosmos DB SQL API .NET SDK demos");
            stringBuilder.AppendLine("DB Databases");
            stringBuilder.AppendLine("CO Collections");
            stringBuilder.AppendLine("DO Documents");
            stringBuilder.AppendLine("IX Indexing");
            stringBuilder.AppendLine("UP Users & Permissions");
            stringBuilder.AppendLine("SP Stored Procedures");
            stringBuilder.AppendLine("TR Triggers");
            stringBuilder.AppendLine("UF User-Defined Functions");
            stringBuilder.AppendLine("C  Cleanup");
            stringBuilder.AppendLine("Q  Quit");

            string prompt = stringBuilder.ToString();

            Console.WriteLine(prompt);
        }

        private static async Task RunDemo(Func<Task> demoMethod)
        {
            try
            {
                await demoMethod();
            }
            catch (Exception exception)
            {
                string message = exception.Message;

                while (exception.InnerException != null)
                {
                    exception = exception.InnerException;
                    message += Environment.NewLine + exception.Message;
                }

                Console.WriteLine($"Exception: {message}");
            }
        }
    }
}
