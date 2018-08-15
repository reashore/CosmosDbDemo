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
		public static Uri MyDbDatabaseUri => UriFactory.CreateDatabaseUri("mydb");

		public static async Task Run()
		{
			Debugger.Break();

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				ListCollections(client);

                const string collection1 = "MyCollection1";
                const string collection2 = "MyCollection2";

                await CreateCollection(client, collection1);
				await CreateCollection(client, collection2);
				ListCollections(client);

				await DeleteCollection(client, collection1);
				await DeleteCollection(client, collection2);
			}
		}

		private static void ListCollections(IDocumentClient client)
		{
			Console.WriteLine(">>> View Collections in mydb <<<");

			List<DocumentCollection> collections = client.CreateDocumentCollectionQuery(MyDbDatabaseUri).ToList();

			int count = 0;
			foreach (DocumentCollection collection in collections)
			{
				count++;
				Console.WriteLine();
				Console.WriteLine($" Collection #{count}");

				PrintCollection(collection);
			}

			Console.WriteLine($"Total collections in mydb database: {collections.Count}");
		}

		private static void PrintCollection(Resource collection)
		{
			Console.WriteLine($"    Collection ID: {collection.Id}");
			Console.WriteLine($"      Resource ID: {collection.ResourceId}");
			Console.WriteLine($"        Self Link: {collection.SelfLink}");
			Console.WriteLine($"            E-Tag: {collection.ETag}");
			Console.WriteLine($"        Timestamp: {collection.Timestamp}");
		}

		private static async Task CreateCollection(IDocumentClient client, string collectionId, int reservedRUs = 1000, string partitionKey = "/partitionKey")
		{
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
			PrintCollection(collection);
		}

		private static async Task DeleteCollection(IDocumentClient client, string collectionId)
		{
			Console.WriteLine($">>> Delete Collection {collectionId} in mydb <<<");

			Uri collectionUri = UriFactory.CreateDocumentCollectionUri("mydb", collectionId);
			ResourceResponse<DocumentCollection> resourceResponse = await client.DeleteDocumentCollectionAsync(collectionUri);
		    //DocumentCollection documentCollection = resourceResponse.Resource;

            //Console.WriteLine($"Deleted collection {collectionId} from database mydb");
		}
	}
}
