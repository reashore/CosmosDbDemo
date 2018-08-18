using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace CosmosDbDemo.Demos
{
	public static class TriggersDemo
	{
		public static Uri MyStoreCollectionUri => UriFactory.CreateDocumentCollectionUri("mydb", "mystore");

		public static async Task Run()
		{
			Debugger.Break();

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await CreateTriggers(client);

				ViewTriggers(client);

				await Execute_trgValidateDocument(client);
				await Execute_trgUpdateMetadata(client);

				await DeleteTriggers(client);
			}
		}

		private static async Task CreateTriggers(IDocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Triggers <<<");
			Console.WriteLine();

			// Create pre-trigger
		    string triggerFileName = @"..\..\Server\trgValidateDocument.js";
            string trgValidateDocument = File.ReadAllText(triggerFileName);
			await CreateTrigger(client, "trgValidateDocument", trgValidateDocument, TriggerType.Pre, TriggerOperation.All);

			// Create post-trigger
		    triggerFileName = @"..\..\Server\trgUpdateMetadata.js";
            string trgUpdateMetadata = File.ReadAllText(triggerFileName);
			await CreateTrigger(client, "trgUpdateMetadata", trgUpdateMetadata, TriggerType.Post, TriggerOperation.Create);
		}

		private static async Task<Trigger> CreateTrigger(
			IDocumentClient client,
			string triggerId,
			string triggerBody,
			TriggerType triggerType,
			TriggerOperation triggerOperation)
		{
			Trigger triggerDefinition = new Trigger
			{
				Id = triggerId,
				Body = triggerBody,
				TriggerType = triggerType,
				TriggerOperation = triggerOperation
			};

			ResourceResponse<Trigger> result = await client.CreateTriggerAsync(MyStoreCollectionUri, triggerDefinition);
			Trigger trigger = result.Resource;
			Console.WriteLine($" Created trigger {trigger.Id}; RID: {trigger.ResourceId}");

			return trigger;
		}

		private static void ViewTriggers(IDocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Triggers <<<");
			Console.WriteLine();

			List<Trigger> triggers = client.CreateTriggerQuery(MyStoreCollectionUri).ToList();

			foreach (Trigger trigger in triggers)
			{
				Console.WriteLine($" Trigger: {trigger.Id};");
				Console.WriteLine($" RID: {trigger.ResourceId};");
				Console.WriteLine($" Type: {trigger.TriggerType};");
				Console.WriteLine($" Operation: {trigger.TriggerOperation}");
				Console.WriteLine();
			}
		}

		private static async Task Execute_trgValidateDocument(DocumentClient client)
		{
			// Create three documents
			string doc1Link = await CreateDocumentWithValidation(client, "mon");		// Monday
			string doc2Link = await CreateDocumentWithValidation(client, "THURS");		// Thursday
		    // ReSharper disable once UnusedVariable
		    string doc3Link = await CreateDocumentWithValidation(client, "sonday");	// error - won't get created

			// Update one of them
			await UpdateDocumentWithValidation(client, doc2Link, "FRI");			// Thursday > Friday

			// Delete them
			RequestOptions requestOptions = new RequestOptions { PartitionKey = new PartitionKey("12345") };
			await client.DeleteDocumentAsync(doc1Link, requestOptions);
			await client.DeleteDocumentAsync(doc2Link, requestOptions);
		}

		private static async Task<string> CreateDocumentWithValidation(IDocumentClient client, string weekdayOff)
		{
			dynamic documentDefinition = new
			{
				name = "John Doe",
				address = new { postalCode = "12345" },
			    // ReSharper disable once RedundantAnonymousTypePropertyName
			    weekdayOff = weekdayOff
			};

			RequestOptions options = new RequestOptions { PreTriggerInclude = new[] { "trgValidateDocument" } };

			try
			{
				dynamic result = await client.CreateDocumentAsync(MyStoreCollectionUri, documentDefinition, options);
				dynamic document = result.Resource;

				Console.WriteLine(" Result:");
				Console.WriteLine($"  Id = {document.id}");
				Console.WriteLine($"  Weekday off = {document.weekdayOff}");
				Console.WriteLine($"  Weekday # off = {document.weekdayNumberOff}");
				Console.WriteLine();

				return document._self;
			}
			catch (DocumentClientException exception)
			{
				Console.WriteLine($"Error: {exception.Message}");
				Console.WriteLine();

				return null;
			}
		}

		private static async Task UpdateDocumentWithValidation(DocumentClient client, string documentLink, string weekdayOff)
		{
			string sql = $"SELECT * FROM c WHERE c._self = '{documentLink}'";
			FeedOptions feedOptions = new FeedOptions { EnableCrossPartitionQuery = true };
			dynamic document = client.CreateDocumentQuery(MyStoreCollectionUri, sql, feedOptions).AsEnumerable().FirstOrDefault();

		    if (document == null)
		    {
		        return;

		    }

			document.weekdayOff = weekdayOff;

			RequestOptions options = new RequestOptions { PreTriggerInclude = new[] { "trgValidateDocument" } };
			dynamic result = await client.ReplaceDocumentAsync(document._self, document, options);
			document = result.Resource;

			Console.WriteLine(" Result:");
			Console.WriteLine($"  Id = {document.id}");
			Console.WriteLine($"  Weekday off = {document.weekdayOff}");
			Console.WriteLine($"  Weekday # off = {document.weekdayNumberOff}");
			Console.WriteLine();
		}

		private static async Task Execute_trgUpdateMetadata(DocumentClient client)
		{
			// Show no metadata documents
			ViewMetaDocs(client);

			// Create a bunch of documents across two partition keys
			List<dynamic> docs = new List<dynamic>
			{
				// 11229
				new { id = "11229a", address = new { postalCode = "11229" }, name = "New Customer ABCD" },
				new { id = "11229b", address = new { postalCode = "11229" }, name = "New Customer ABC" },
				new { id = "11229c", address = new { postalCode = "11229" }, name = "New Customer AB" },			// smallest
				new { id = "11229d", address = new { postalCode = "11229" }, name = "New Customer ABCDEF" },
				new { id = "11229e", address = new { postalCode = "11229" }, name = "New Customer ABCDEFG" },		// largest
				new { id = "11229f", address = new { postalCode = "11229" }, name = "New Customer ABCDE" },
				// 11235
				new { id = "11235a", address = new { postalCode = "11235" }, name = "New Customer AB" },
				new { id = "11235b", address = new { postalCode = "11235" }, name = "New Customer ABCDEFGHIJKL" },	// largest
				new { id = "11235c", address = new { postalCode = "11235" }, name = "New Customer ABC" },
				new { id = "11235d", address = new { postalCode = "11235" }, name = "New Customer A" },				// smallest
				new { id = "11235e", address = new { postalCode = "11235" }, name = "New Customer ABC" },
				new { id = "11235f", address = new { postalCode = "11235" }, name = "New Customer ABCDE" },
			};

			RequestOptions options = new RequestOptions { PostTriggerInclude = new[] { "trgUpdateMetadata" } };
			foreach (dynamic doc in docs)
			{
				await client.CreateDocumentAsync(MyStoreCollectionUri, doc, options);
			}

			// Show two metadata documents
			ViewMetaDocs(client);

			// Cleanup
			const string sql = @"
				SELECT c._self, c.address.postalCode
				FROM c
				WHERE c.address.postalCode IN('11229', '11235')";

			FeedOptions feedOptions = new FeedOptions { EnableCrossPartitionQuery = true };
			List<dynamic> documentKeys = client.CreateDocumentQuery(MyStoreCollectionUri, sql, feedOptions).ToList();
			foreach (dynamic documentKey in documentKeys)
			{
				RequestOptions requestOptions = new RequestOptions { PartitionKey = new PartitionKey(documentKey.postalCode) };
				await client.DeleteDocumentAsync(documentKey._self, requestOptions);
			}
		}

		private static void ViewMetaDocs(IDocumentClient client)
		{
			const string sql = @"SELECT * FROM c WHERE c.isMetaDoc";

			FeedOptions feedOptions = new FeedOptions { EnableCrossPartitionQuery = true };
			List<dynamic> metaDocs = client.CreateDocumentQuery(MyStoreCollectionUri, sql, feedOptions).ToList();

			Console.WriteLine();
			Console.WriteLine($" Found {metaDocs.Count} metadata documents:");
			foreach (dynamic metaDoc in metaDocs)
			{
				Console.WriteLine();
				Console.WriteLine($"  MetaDoc ID: {metaDoc.id}");
				Console.WriteLine($"  Metadata for: {metaDoc.address.postalCode}");
				Console.WriteLine($"  Smallest doc size: {metaDoc.minSize} ({metaDoc.minSizeId})");
				Console.WriteLine($"  Largest doc size: {metaDoc.maxSize} ({metaDoc.maxSizeId})");
				Console.WriteLine($"  Total doc size: {metaDoc.totalSize}");
			}
		}

		private static async Task DeleteTriggers(IDocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Triggers <<<");
			Console.WriteLine();

			await DeleteTrigger(client, "trgValidateDocument");
			await DeleteTrigger(client, "trgUpdateMetadata");
		}

		private static async Task DeleteTrigger(IDocumentClient client, string triggerId)
		{
			Uri triggerUri = UriFactory.CreateTriggerUri("mydb", "mystore", triggerId);

			await client.DeleteTriggerAsync(triggerUri);

			Console.WriteLine($"Deleted trigger: {triggerId}");
		}
	}
}
