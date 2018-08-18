using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace CosmosDbDemo.Demos
{
	public static class StoredProceduresDemo
	{
		public static Uri MyStoreCollectionUri => UriFactory.CreateDocumentCollectionUri("mydb", "mystore");

		public static async Task Run()
		{
			Debugger.Break();

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await CreateStoredProcedures(client);

				ListStoredProcedures(client);

				await ExecuteStoredProcedures(client);

				await DeleteStoredProcedures(client);
			}
		}

		private static async Task CreateStoredProcedures(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Stored Procedures <<<");
			Console.WriteLine();

			await CreateStoredProcedure(client, "spHelloWorld");
			await CreateStoredProcedure(client, "spSetNorthAmerica");
			await CreateStoredProcedure(client, "spEnsureUniqueId");
			await CreateStoredProcedure(client, "spBulkInsert");
			await CreateStoredProcedure(client, "spBulkDelete");
		}

		private static async Task<StoredProcedure> CreateStoredProcedure(DocumentClient client, string sprocId)
		{
			string sprocBody = File.ReadAllText($@"..\..\Server\{sprocId}.js");

			StoredProcedure sprocDefinition = new StoredProcedure
			{
				Id = sprocId,
				Body = sprocBody
			};

			ResourceResponse<StoredProcedure> result = await client.CreateStoredProcedureAsync(MyStoreCollectionUri, sprocDefinition);
			StoredProcedure sproc = result.Resource;
			Console.WriteLine($"Created stored procedure {sproc.Id}; RID: {sproc.ResourceId}");

			return result;
		}

		private static void ListStoredProcedures(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Stored Procedures <<<");
			Console.WriteLine();

			List<StoredProcedure> sprocs = client
				.CreateStoredProcedureQuery(MyStoreCollectionUri)
				.ToList();

			foreach (StoredProcedure sproc in sprocs)
			{
				Console.WriteLine($"Stored procedure {sproc.Id}; RID: {sproc.ResourceId}");
			}
		}

		private static async Task ExecuteStoredProcedures(DocumentClient client)
		{
			await Execute_spHelloWorld(client);
			await Execute_spSetNorthAmerica1(client);
			await Execute_spSetNorthAmerica2(client);
			await Execute_spSetNorthAmerica3(client);
			await Execute_spEnsureUniqueId(client);
			await Execute_spBulkInsert(client);
			await Execute_spBulkDelete(client);
		}

		private static async Task Execute_spHelloWorld(IDocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spHelloWorld stored procedure");

			Uri uri = UriFactory.CreateStoredProcedureUri("mydb", "mystore", "spHelloWorld");
			RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey(string.Empty) };
			StoredProcedureResponse<string> result = await client.ExecuteStoredProcedureAsync<string>(uri, options);
			string message = result.Response;

			Console.WriteLine($"Result: {message}");
		}

		private static async Task Execute_spSetNorthAmerica1(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spSetNorthAmerica (country = United States)");

			// Should succeed with isNorthAmerica = true
			dynamic documentDefinition = new
			{
				name = "John Doe",
				address = new
				{
					countryRegionName = "United States",
					postalCode = "12345"
				}
			};
			Uri uri = UriFactory.CreateStoredProcedureUri("mydb", "mystore", "spSetNorthAmerica");
			RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey("12345") };
			dynamic result = await client.ExecuteStoredProcedureAsync<object>(uri, options, documentDefinition, true);
			dynamic document = result.Response;

			dynamic id = document.id;
			dynamic country = document.address.countryRegionName;
			dynamic isNorthAmerica = document.address.isNorthAmerica;

			Console.WriteLine("Result:");
			Console.WriteLine($" Id = {id}");
			Console.WriteLine($" Country = {country}");
			Console.WriteLine($" Is North America = {isNorthAmerica}");

			string documentLink = document._self;
			await client.DeleteDocumentAsync(documentLink, options);
		}

		private static async Task Execute_spSetNorthAmerica2(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spSetNorthAmerica (country = United Kingdom)");

			// Should succeed with isNorthAmerica = false
			dynamic documentDefinition = new
			{
				name = "John Doe",
				address = new
				{
					countryRegionName = "United Kingdom",
					postalCode = "RG41 1QW"
				}
			};
			Uri uri = UriFactory.CreateStoredProcedureUri("mydb", "mystore", "spSetNorthAmerica");
			RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey("RG41 1QW") };
			dynamic result = await client.ExecuteStoredProcedureAsync<object>(uri, options, documentDefinition, true);
			dynamic document = result.Response;

			// Deserialize new document as JObject (use dictionary-style indexers to access dynamic properties)
			dynamic documentObject = JsonConvert.DeserializeObject(document.ToString());

			dynamic id = documentObject["id"];
			dynamic country = documentObject["address"]["countryRegionName"];
			dynamic isNorthAmerica = documentObject["address"]["isNorthAmerica"];

			Console.WriteLine("Result:");
			Console.WriteLine($" Id = {id}");
			Console.WriteLine($" Country = {country}");
			Console.WriteLine($" Is North America = {isNorthAmerica}");

			string documentLink = document._self;
			await client.DeleteDocumentAsync(documentLink, options);
		}

		private static async Task Execute_spSetNorthAmerica3(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spSetNorthAmerica (no country)");

			// Should fail with no country and enforceSchema = true
			try
			{
				dynamic documentDefinition = new
				{
					name = "James Smith",
					address = new
					{
						postalCode = "12345"
					}
				};
				Uri uri = UriFactory.CreateStoredProcedureUri("mydb", "mystore", "spSetNorthAmerica");
				RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey("12345") };
			    // ReSharper disable once UnusedVariable
			    dynamic result = await client.ExecuteStoredProcedureAsync<object>(uri, options, documentDefinition, true);
			}
			catch (DocumentClientException exception)
			{
				Console.WriteLine($"Error: {exception.Message}");
			}
		}

		private static async Task Execute_spEnsureUniqueId(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spEnsureUniqueId");

			dynamic documentDefinition1 = new { id = "DUPEJ", name = "James Dupe", address = new { postalCode = "12345" } };
			dynamic documentDefinition2 = new { id = "DUPEJ", name = "John Dupe", address = new { postalCode = "12345" } };
			dynamic documentDefinition3 = new { id = "DUPEJ", name = "Justin Dupe", address = new { postalCode = "12345" } };

			Uri uri = UriFactory.CreateStoredProcedureUri("mydb", "mystore", "spEnsureUniqueId");
			RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey("12345") };

			dynamic result1 = await client.ExecuteStoredProcedureAsync<object>(uri, options, documentDefinition1);
			dynamic document1 = result1.Response;
			Console.WriteLine($"New document ID: {document1.id}");

			dynamic result2 = await client.ExecuteStoredProcedureAsync<object>(uri, options, documentDefinition2);
			dynamic document2 = result2.Response;
			Console.WriteLine($"New document ID: {document2.id}");

			dynamic result3 = await client.ExecuteStoredProcedureAsync<object>(uri, options, documentDefinition3);
			dynamic document3 = result3.Response;
			Console.WriteLine($"New document ID: {document3.id}");

			// cleanup
			await client.DeleteDocumentAsync(document1._self.ToString(), options);
			await client.DeleteDocumentAsync(document2._self.ToString(), options);
			await client.DeleteDocumentAsync(document3._self.ToString(), options);
		}

		private static async Task Execute_spBulkInsert(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spBulkInsert");

			List<dynamic> docs = new List<dynamic>();
			const int total = 5000;
			for (int i = 1; i <= total; i++)
			{
				dynamic doc = new
				{
					name = $"Bulk inserted doc {i}",
					address = new
					{
						postalCode = "12345"
					}
				};
				docs.Add(doc);
			}

			Uri uri = UriFactory.CreateStoredProcedureUri("mydb", "mystore", "spBulkInsert");
			RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey("12345") };

			int totalInserted = 0;
			while (totalInserted < total)
			{
				StoredProcedureResponse<int> result = await client.ExecuteStoredProcedureAsync<int>(uri, options, docs);
				int inserted = result.Response;
				totalInserted += inserted;
				int remaining = total - totalInserted;
				Console.WriteLine($"Inserted {inserted} documents ({totalInserted} total, {remaining} remaining)");
				docs = docs.GetRange(inserted, docs.Count - inserted);
			}
		}

		private static async Task Execute_spBulkDelete(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spBulkDelete");

			// query retrieves self-links for documents to bulk-delete
			string sql = "SELECT VALUE c._self FROM c WHERE STARTSWITH(c.name, 'Bulk inserted doc ') = true";
			int count = await Execute_spBulkDelete(client, sql);
			Console.WriteLine($"Deleted bulk inserted documents; count: {count}");
			Console.WriteLine();
		}

		private static async Task<int> Execute_spBulkDelete(DocumentClient client, string sql)
		{
			Uri uri = UriFactory.CreateStoredProcedureUri("mydb", "mystore", "spBulkDelete");
			RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey("12345") };

			bool continuationFlag = true;
			int totalDeleted = 0;
			while (continuationFlag)
			{
				StoredProcedureResponse<BulkDeleteResponse> result = await client.ExecuteStoredProcedureAsync<BulkDeleteResponse>(uri, options, sql);
				BulkDeleteResponse response = result.Response;
				continuationFlag = response.ContinuationFlag;
				int deleted = response.Count;
				totalDeleted += deleted;
				Console.WriteLine($"Deleted {deleted} documents ({totalDeleted} total, more: {continuationFlag})");
			}

			return totalDeleted;
		}

		private static async Task DeleteStoredProcedures(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Stored Procedures <<<");
			Console.WriteLine();

			await DeleteStoredProcedure(client, "spHelloWorld");
			await DeleteStoredProcedure(client, "spSetNorthAmerica");
			await DeleteStoredProcedure(client, "spEnsureUniqueId");
			await DeleteStoredProcedure(client, "spBulkInsert");
			await DeleteStoredProcedure(client, "spBulkDelete");
		}

		private static async Task DeleteStoredProcedure(DocumentClient client, string sprocId)
		{
			Uri sprocUri = UriFactory.CreateStoredProcedureUri("mydb", "mystore", sprocId);

			await client.DeleteStoredProcedureAsync(sprocUri);

			Console.WriteLine($"Deleted stored procedure: {sprocId}");
		}

	}
}
