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
	public static class UserDefinedFunctionsDemo
	{
		public static Uri MyStoreCollectionUri =>
			UriFactory.CreateDocumentCollectionUri("mydb", "mystore");

		public static async Task Run()
		{
			Debugger.Break();

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await CreateUserDefinedFunctions(client);

				ViewUserDefinedFunctions(client);

				Execute_udfRegEx(client);
				Execute_udfIsNorthAmerica(client);
				Execute_udfFormatCityStateZip(client);

				await DeleteUserDefinedFunctions(client);
			}
		}

		private static async Task CreateUserDefinedFunctions(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create User Defined Functions <<<");
			Console.WriteLine();

			await CreateUserDefinedFunction(client, "udfRegEx");
			await CreateUserDefinedFunction(client, "udfIsNorthAmerica");
			await CreateUserDefinedFunction(client, "udfFormatCityStateZip");
		}

		private static async Task<UserDefinedFunction> CreateUserDefinedFunction(DocumentClient client, string udfId)
		{
			string udfBody = File.ReadAllText($@"..\..\Server\{udfId}.js");
			UserDefinedFunction udfDefinition = new UserDefinedFunction
			{
				Id = udfId,
				Body = udfBody
			};

			ResourceResponse<UserDefinedFunction> result = await client.CreateUserDefinedFunctionAsync(MyStoreCollectionUri, udfDefinition);
			UserDefinedFunction udf = result.Resource;
			Console.WriteLine($" Created user defined function {udf.Id}; RID: {udf.ResourceId}");

			return udf;
		}

		private static void ViewUserDefinedFunctions(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View UDFs <<<");
			Console.WriteLine();

			List<UserDefinedFunction> udfs = client
				.CreateUserDefinedFunctionQuery(MyStoreCollectionUri)
				.ToList();

			foreach (UserDefinedFunction udf in udfs)
			{
				Console.WriteLine($" User defined function {udf.Id}; RID: {udf.ResourceId}");
			}
		}

		private static void Execute_udfRegEx(DocumentClient client)
		{
			const string sql = "SELECT c.id, c.name FROM c WHERE udf.udfRegEx(c.name, 'Rental') != null";

			Console.WriteLine();
			Console.WriteLine("Querying for Rental customers");
			FeedOptions options = new FeedOptions { EnableCrossPartitionQuery = true };
			List<dynamic> documents = client.CreateDocumentQuery(MyStoreCollectionUri, sql, options).ToList();

			Console.WriteLine($"Found {documents.Count} Rental customers:");
			foreach (dynamic document in documents)
			{
				Console.WriteLine($" {document.name} ({document.id})");
			}
		}

		private static void Execute_udfIsNorthAmerica(DocumentClient client)
		{
			string sql = @"
				SELECT c.name, c.address.countryRegionName
				FROM c
				WHERE udf.udfIsNorthAmerica(c.address.countryRegionName) = true";

			Console.WriteLine();
			Console.WriteLine("Querying for North American customers");
			FeedOptions options = new FeedOptions { EnableCrossPartitionQuery = true };
			List<dynamic> documents = client.CreateDocumentQuery(MyStoreCollectionUri, sql, options).ToList();

			Console.WriteLine($"Found {documents.Count} North American customers; first 20:");
			foreach (dynamic document in documents.Take(20))
			{
				Console.WriteLine($" {document.name}, {document.countryRegionName}");
			}

			sql = @"
				SELECT c.name, c.address.countryRegionName
				FROM c
				WHERE udf.udfIsNorthAmerica(c.address.countryRegionName) = false";

			Console.WriteLine();
			Console.WriteLine("Querying for non North American customers");
			documents = client.CreateDocumentQuery(MyStoreCollectionUri, sql, options).ToList();

			Console.WriteLine($"Found {documents.Count} non North American customers; first 20:");
			foreach (dynamic document in documents.Take(20))
			{
				Console.WriteLine($" {document.name}, {document.countryRegionName}");
			}
		}

		private static void Execute_udfFormatCityStateZip(DocumentClient client)
		{
			string sql = "SELECT c.name, udf.udfFormatCityStateZip(c) AS csz FROM c";

			Console.WriteLine();
			Console.WriteLine("Listing names with city, state, zip (first 20)");

			FeedOptions options = new FeedOptions { EnableCrossPartitionQuery = true };
			List<dynamic> documents = client.CreateDocumentQuery(MyStoreCollectionUri, sql, options).ToList();
			foreach (dynamic document in documents.Take(20))
			{
				Console.WriteLine($" {document.name} located in {document.csz}");
			}
		}

		private static async Task DeleteUserDefinedFunctions(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete User Defined Functions <<<");
			Console.WriteLine();

			await DeleteUserDefinedFunction(client, "udfRegEx");
			await DeleteUserDefinedFunction(client, "udfIsNorthAmerica");
			await DeleteUserDefinedFunction(client, "udfFormatCityStateZip");
		}

		private static async Task DeleteUserDefinedFunction(DocumentClient client, string udfId)
		{
			Uri udfUri = UriFactory.CreateUserDefinedFunctionUri("mydb", "mystore", udfId);

			await client.DeleteUserDefinedFunctionAsync(udfUri);

			Console.WriteLine($"Deleted user defined function: {udfId}");
		}
	}
}
