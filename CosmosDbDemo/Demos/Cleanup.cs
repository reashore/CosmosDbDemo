using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace CosmosDbDemo.Demos
{
	public static class Cleanup
	{
		public static async Task Run()
		{
			Console.WriteLine(">>> Cleanup <<<");

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				var collectionUri = await DeleteDocuments(client);
			    await DeleteStoredProcedures(client, collectionUri);
			    await DeleteUserDefinedFunctions(client, collectionUri);
			    await DeleteTriggers(client, collectionUri);
			    await DeleteUsers(client);
			}
		}

	    private static async Task<Uri> DeleteDocuments(DocumentClient client)
	    {
	        Console.WriteLine("Deleting documents");
	        const string sql = @"
					SELECT c._self, c.address.postalCode
					FROM c
					WHERE
						STARTSWITH(c.name, 'New Customer') OR
						STARTSWITH(c.id, '_meta') OR
						IS_DEFINED(c.weekdayOff)";

	        Uri collectionUri = UriFactory.CreateDocumentCollectionUri("mydb", "mystore");
	        FeedOptions feedOptions = new FeedOptions { EnableCrossPartitionQuery = true };
	        IEnumerable<dynamic> documentKeys = client.CreateDocumentQuery(collectionUri, sql, feedOptions).AsEnumerable();

	        foreach (dynamic documentKey in documentKeys)
	        {
	            RequestOptions requestOptions = new RequestOptions { PartitionKey = new PartitionKey(documentKey.postalCode) };
	            await client.DeleteDocumentAsync(documentKey._self, requestOptions);
	        }

	        return collectionUri;
	    }

	    private static async Task DeleteStoredProcedures(IDocumentClient client, Uri collectionUri)
	    {
	        Console.WriteLine("Deleting stored procedures");
	        IEnumerable<StoredProcedure> storedProcedures = client.CreateStoredProcedureQuery(collectionUri).AsEnumerable();

            foreach (StoredProcedure storedProcedure in storedProcedures)
	        {
	            await client.DeleteStoredProcedureAsync(storedProcedure.SelfLink);
	        }
	    }

	    private static async Task DeleteUserDefinedFunctions(IDocumentClient client, Uri collectionUri)
	    {
	        Console.WriteLine("Deleting user defined functions");
	        IEnumerable<UserDefinedFunction> userDefinedFunctions = client.CreateUserDefinedFunctionQuery(collectionUri).AsEnumerable();

	        foreach (UserDefinedFunction userDefinedFunction in userDefinedFunctions)
	        {
	            await client.DeleteUserDefinedFunctionAsync(userDefinedFunction.SelfLink);
	        }
	    }

        private static async Task DeleteTriggers(IDocumentClient client, Uri collectionUri)
	    {
	        Console.WriteLine("Deleting triggers");
	        IEnumerable<Trigger> triggers = client.CreateTriggerQuery(collectionUri).AsEnumerable();

	        foreach (Trigger trigger in triggers)
	        {
	            await client.DeleteTriggerAsync(trigger.SelfLink);
	        }
	    }

	    private static async Task DeleteUsers(IDocumentClient client)
	    {
	        Console.WriteLine("Deleting users");
	        Uri databaseUri = UriFactory.CreateDatabaseUri("mydb");
	        IEnumerable<User> users = client.CreateUserQuery(databaseUri).AsEnumerable();

	        foreach (User user in users)
	        {
	            await client.DeleteUserAsync(user.SelfLink);
	        }
	    }
    }
}
