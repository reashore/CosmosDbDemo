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
	public static class CollectionsDemo
	{
		public static Uri MyDbDatabaseUri =>
			UriFactory.CreateDatabaseUri("mydb");

		public static async Task Run()
		{
			Debugger.Break();

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				ViewCollections(client);

				await CreateCollection(client, "MyCollection1");
				await CreateCollection(client, "MyCollection2", 25000);
				ViewCollections(client);

				await DeleteCollection(client, "MyCollection1");
				await DeleteCollection(client, "MyCollection2");
			}
		}

		private static void ViewCollections(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Collections in mydb <<<");

			List<DocumentCollection> collections = client
				.CreateDocumentCollectionQuery(MyDbDatabaseUri)
				.ToList();

			int i = 0;
			foreach (DocumentCollection collection in collections)
			{
				i++;
				Console.WriteLine();
				Console.WriteLine($" Collection #{i}");
				ViewCollection(collection);
			}

			Console.WriteLine();
			Console.WriteLine($"Total collections in mydb database: {collections.Count}");
		}

		private static void ViewCollection(DocumentCollection collection)
		{
			Console.WriteLine($"    Collection ID: {collection.Id}");
			Console.WriteLine($"      Resource ID: {collection.ResourceId}");
			Console.WriteLine($"        Self Link: {collection.SelfLink}");
			Console.WriteLine($"            E-Tag: {collection.ETag}");
			Console.WriteLine($"        Timestamp: {collection.Timestamp}");
		}

		private static async Task CreateCollection(
			DocumentClient client,
			string collectionId,
			int reservedRUs = 1000,
			string partitionKey = "/partitionKey")
		{
			Console.WriteLine();
			Console.WriteLine($">>> Create Collection {collectionId} in mydb <<<");
			Console.WriteLine();
			Console.WriteLine($" Throughput: {reservedRUs} RU/sec");
			Console.WriteLine($" Partition key: {partitionKey}");
			Console.WriteLine();

			PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
			partitionKeyDefinition.Paths.Add(partitionKey);

			DocumentCollection collectionDefinition = new DocumentCollection
			{
				Id = collectionId,
				PartitionKey = partitionKeyDefinition
			};
			RequestOptions options = new RequestOptions { OfferThroughput = reservedRUs };

			ResourceResponse<DocumentCollection> result = await client.CreateDocumentCollectionAsync(MyDbDatabaseUri, collectionDefinition, options);
			DocumentCollection collection = result.Resource;

			Console.WriteLine("Created new collection");
			ViewCollection(collection);
		}

		private static async Task DeleteCollection(DocumentClient client, string collectionId)
		{
			Console.WriteLine();
			Console.WriteLine($">>> Delete Collection {collectionId} in mydb <<<");

			Uri collectionUri = UriFactory.CreateDocumentCollectionUri("mydb", collectionId);
			await client.DeleteDocumentCollectionAsync(collectionUri);

			Console.WriteLine($"Deleted collection {collectionId} from database mydb");
		}

	}
}
