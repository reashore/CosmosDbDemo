using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace CosmosDbDemo.Demos
{
	public static class DatabasesDemo
	{
		public static async Task Run()
		{
			Debugger.Break();

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				ViewDatabases(client);

				await CreateDatabase(client);
				ViewDatabases(client);

				await DeleteDatabase(client);
			}
		}

		private static void ViewDatabases(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Databases <<<");

			List<Database> databases = client.CreateDatabaseQuery().ToList();
			foreach (Database database in databases)
			{
				Console.WriteLine($" Database Id: {database.Id}; Rid: {database.ResourceId}");
			}

			Console.WriteLine();
			Console.WriteLine($"Total databases: {databases.Count}");
		}

		private static async Task CreateDatabase(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Database <<<");

			Database databaseDefinition = new Database { Id = "MyNewDatabase" };
			ResourceResponse<Database> result = await client.CreateDatabaseAsync(databaseDefinition);
			Database database = result.Resource;

			Console.WriteLine($" Database Id: {database.Id}; Rid: {database.ResourceId}");
		}

		private static async Task DeleteDatabase(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Database <<<");

			Uri databaseUri = UriFactory.CreateDatabaseUri("MyNewDatabase");
			await client.DeleteDatabaseAsync(databaseUri);
		}

	}
}
