using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace CosmosDbDemo.Demos
{
	public static class DatabaseDemo
	{
		public static async Task Run()
		{
			Debugger.Break();

		    string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
		    string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

            using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				ListDatabases(client);

                const string databaseId = "DatabaseId1";
				await CreateDatabase(client, databaseId);
				ListDatabases(client);

				await DeleteDatabase(client, databaseId);
			}
		}

        private static void ListDatabases(IDocumentClient client)
		{
			Console.WriteLine(">>> List Databases <<<");

			List<Database> databases = client.CreateDatabaseQuery().ToList();

			foreach (Database database in databases)
			{
			    PrintDatabase(database);
			}

			Console.WriteLine($"Total databases: {databases.Count}");
		}

	    private static void PrintDatabase(Database database)
	    {
	        StringBuilder stringBuilder = new StringBuilder();

	        stringBuilder.AppendLine($"Id = {database.Id}");
	        stringBuilder.AppendLine($"ResourceId = {database.ResourceId}");
	        stringBuilder.AppendLine($"SelfLink = {database.SelfLink}");
	        stringBuilder.AppendLine($"ETag = {database.ETag}");
	        stringBuilder.AppendLine($"Timestamp = {database.Timestamp}");
	        stringBuilder.AppendLine($"AltLink = {database.AltLink}");
	        stringBuilder.AppendLine($"CollectionsLink = {database.CollectionsLink}");
	        stringBuilder.AppendLine($"UsersLink = {database.UsersLink}");

	        string databaseInfo = stringBuilder.ToString();

	        Console.WriteLine(databaseInfo);
	    }

		private static async Task CreateDatabase(IDocumentClient client, string databaseId)
		{
			Console.WriteLine(">>> Create Database <<<");

			Database databaseDefinition = new Database { Id = databaseId };
			ResourceResponse<Database> result = await client.CreateDatabaseAsync(databaseDefinition);
			Database database = result.Resource;

		    PrintDatabase(database);
		}

		private static async Task DeleteDatabase(IDocumentClient client, string databaseId)
		{
			Console.WriteLine(">>> Delete Database <<<");

			Uri databaseUri = UriFactory.CreateDatabaseUri(databaseId);
			ResourceResponse<Database> foo = await client.DeleteDatabaseAsync(databaseUri);
		    Database database = foo.Resource;

		    PrintDatabase(database);
		}
	}
}
