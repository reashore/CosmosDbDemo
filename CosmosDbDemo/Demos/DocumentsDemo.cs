using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace CosmosDbDemo.Demos
{
	public static class DocumentsDemo
	{
		public static Uri MyStoreCollectionUri => UriFactory.CreateDocumentCollectionUri("mydb", "mystore");

		public static async Task Run()
		{
			Debugger.Break();

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await CreateDocuments(client);

				QueryDocumentsWithSql(client);
				await QueryDocumentsWithPaging(client);
				QueryDocumentsWithLinq(client);

				await ReplaceDocuments(client);

				await DeleteDocuments(client);
			}
		}

		private static async Task CreateDocuments(IDocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Documents <<<");
			Console.WriteLine();

			dynamic dynamicDocumentDefinition1 = new
			{
				name = "New Customer 1",
				address = new
				{
					addressType = "Main Office",
					addressLine1 = "123 Main Street",
					location = new
					{
						city = "Brooklyn",
						stateProvinceName = "New York"
					},
					postalCode = "11229",
					countryRegionName = "United States"
				}
			};

			Document document1 = await CreateDocument(client, dynamicDocumentDefinition1);
			Console.WriteLine($"Created document {document1.Id} from dynamic object");
			Console.WriteLine();

			const string jsonDocumentDefinition2 = @"
			{
				""name"": ""New Customer 2"",
				""address"": {
					""addressType"": ""Main Office"",
					""addressLine1"": ""123 Main Street"",
					""location"": {
						""city"": ""Brooklyn"",
						""stateProvinceName"": ""New York""
					},
					""postalCode"": ""11229"",
					""countryRegionName"": ""United States""
				}
			}";

			object documentObject2 = JsonConvert.DeserializeObject(jsonDocumentDefinition2);
			Document document2 = await CreateDocument(client, documentObject2);
			Console.WriteLine($"Created document {document2.Id} from JSON string");
			Console.WriteLine();

			Customer pocoDocumentDefinition3 = new Customer
			{
				Name = "New Customer 3",
				Address = new Address
				{
					AddressType = "Main Office",
					AddressLine1 = "123 Main Street",
					Location = new Location
					{
						City = "Brooklyn",
						StateProvinceName = "New York"
					},
					PostalCode = "11229",
					CountryRegionName = "United States"
				}
			};

			Document document3 = await CreateDocument(client, pocoDocumentDefinition3);
			Console.WriteLine($"Created document {document3.Id} from typed object");
			Console.WriteLine();
		}

		private static async Task<Document> CreateDocument(IDocumentClient client, object documentObject)
		{
			ResourceResponse<Document> result = await client.CreateDocumentAsync(MyStoreCollectionUri, documentObject);
			Document document = result.Resource;
			Console.WriteLine($"Created new document: {document.Id}");
			Console.WriteLine(document);
			return result;
		}

		private static void QueryDocumentsWithSql(IDocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Query Documents (SQL) <<<");
			Console.WriteLine();

			Console.WriteLine("Querying for new customer documents (SQL)");
			string sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";
			FeedOptions options = new FeedOptions { EnableCrossPartitionQuery = true };

			// Query for dynamic objects
			List<dynamic> documents = client.CreateDocumentQuery(MyStoreCollectionUri, sql, options).ToList();
			Console.WriteLine($"Found {documents.Count} new documents");
			foreach (dynamic document in documents)
			{
				Console.WriteLine($" Id: {document.id}; Name: {document.name};");

				// Dynamic object can be converted into a defined type...
				dynamic customer = JsonConvert.DeserializeObject<Customer>(document.ToString());
				Console.WriteLine($" City: {customer.Address.Location.City}");
			}
			Console.WriteLine();

			// Or query for defined types; e.g., Customer
			List<Customer> customers = client.CreateDocumentQuery<Customer>(MyStoreCollectionUri, sql, options).ToList();
			Console.WriteLine($"Found {customers.Count} new customers");
			foreach (Customer customer in customers)
			{
				Console.WriteLine($" Id: {customer.Id}; Name: {customer.Name};");
				Console.WriteLine($" City: {customer.Address.Location.City}");
			}
			Console.WriteLine();

			Console.WriteLine("Querying for all documents (SQL)");
			sql = "SELECT * FROM c";
			documents = client.CreateDocumentQuery(MyStoreCollectionUri, sql, options).ToList();

			Console.WriteLine($"Found {documents.Count} documents");
			foreach (dynamic document in documents)
			{
				Console.WriteLine($" Id: {document.id}; Name: {document.name};");
			}
			Console.WriteLine();
		}

		private static async Task QueryDocumentsWithPaging(IDocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Query Documents (paged results) <<<");
			Console.WriteLine();

			Console.WriteLine("Querying for all documents");
			const string sql = "SELECT * FROM c";
			FeedOptions options = new FeedOptions { EnableCrossPartitionQuery = true };

			IDocumentQuery<dynamic> query = client
				.CreateDocumentQuery(MyStoreCollectionUri, sql, options)
				.AsDocumentQuery();

			while (query.HasMoreResults)
			{
				FeedResponse<dynamic> documents = await query.ExecuteNextAsync();
				foreach (dynamic document in documents)
				{
					Console.WriteLine($" Id: {document.id}; Name: {document.name};");
				}
			}
			Console.WriteLine();
		}

		private static void QueryDocumentsWithLinq(IDocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Query Documents (LINQ) <<<");
			Console.WriteLine();

			FeedOptions options = new FeedOptions { EnableCrossPartitionQuery = true };

			Console.WriteLine("Querying for UK customers (LINQ)");
			var query =
				from document in client.CreateDocumentQuery<Customer>(MyStoreCollectionUri, options)
				where document.Address.CountryRegionName == "United Kingdom"
				select new
				{
				    document.Id,
				    document.Name,
				    document.Address.Location.City
				};

			var documents = query.ToList();

			Console.WriteLine($"Found {documents.Count} UK customers");
			foreach (var document in documents)
			{
				dynamic d = document;
				Console.WriteLine($" Id: {d.Id}; Name: {d.Name}; City: {d.City}");
			}
			Console.WriteLine();
		}

		private static async Task ReplaceDocuments(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Replace Documents <<<");
			Console.WriteLine();

			FeedOptions options = new FeedOptions { EnableCrossPartitionQuery = true };

			Console.WriteLine("Querying for documents with 'isNew' flag");
			string sql = "SELECT VALUE COUNT(c) FROM c WHERE c.isNew = true";
			dynamic count = client.CreateDocumentQuery(MyStoreCollectionUri, sql, options).AsEnumerable().First();
			Console.WriteLine($"Documents with 'isNew' flag: {count}");
			Console.WriteLine();

			Console.WriteLine("Querying for documents to be updated");
			sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";
			List<dynamic> documents = client.CreateDocumentQuery(MyStoreCollectionUri, sql, options).ToList();
			Console.WriteLine($"Found {documents.Count} documents to be updated");
			foreach (dynamic document in documents)
			{
				document.isNew = true;
				dynamic result = await client.ReplaceDocumentAsync(document._self, document);
				dynamic updatedDocument = result.Resource;
				Console.WriteLine($"Updated document 'isNew' flag: {updatedDocument.isNew}");
			}
			Console.WriteLine();

			Console.WriteLine("Querying for documents with 'isNew' flag");
			sql = "SELECT VALUE COUNT(c) FROM c WHERE c.isNew = true";
			count = client.CreateDocumentQuery(MyStoreCollectionUri, sql, options).AsEnumerable().First();
			Console.WriteLine($"Documents with 'isNew' flag: {count}");
			Console.WriteLine();
		}

		private static async Task DeleteDocuments(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Documents <<<");
			Console.WriteLine();

			FeedOptions feedOptions = new FeedOptions { EnableCrossPartitionQuery = true };

			Console.WriteLine("Querying for documents to be deleted");
			const string sql = "SELECT c._self, c.address.postalCode FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";
			List<dynamic> documentKeys = client.CreateDocumentQuery(MyStoreCollectionUri, sql, feedOptions).ToList();

			Console.WriteLine($"Found {documentKeys.Count} documents to be deleted");
			foreach (dynamic documentKey in documentKeys)
			{
				RequestOptions requestOptions = new RequestOptions { PartitionKey = new PartitionKey(documentKey.postalCode) };
				await client.DeleteDocumentAsync(documentKey._self, requestOptions);
			}

			Console.WriteLine($"Deleted {documentKeys.Count} new customer documents");
			Console.WriteLine();
		}
	}
}
