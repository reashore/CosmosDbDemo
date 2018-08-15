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
			Console.WriteLine();
			Console.WriteLine(">>> Cleanup <<<");

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				// Delete documents created by demos
				Console.WriteLine("Deleting documents created by demos...");
				const string sql = @"
					SELECT c._self, c.address.postalCode
					FROM c
					WHERE
						STARTSWITH(c.name, 'New Customer') OR
						STARTSWITH(c.id, '_meta') OR
						IS_DEFINED(c.weekdayOff)
				";

				Uri collectionUri = UriFactory.CreateDocumentCollectionUri("mydb", "mystore");
				FeedOptions feedOptions = new FeedOptions { EnableCrossPartitionQuery = true };
				IEnumerable<dynamic> documentKeys = client.CreateDocumentQuery(collectionUri, sql, feedOptions).AsEnumerable();

				foreach (dynamic documentKey in documentKeys)
				{
					RequestOptions requestOptions = new RequestOptions { PartitionKey = new PartitionKey(documentKey.postalCode) };
					await client.DeleteDocumentAsync(documentKey._self, requestOptions);
				}

				IEnumerable<StoredProcedure> sprocs = client.CreateStoredProcedureQuery(collectionUri).AsEnumerable();

				// Delete all stored procedures
				Console.WriteLine("Deleting all stored procedures...");

				foreach (StoredProcedure sproc in sprocs)
				{
					await client.DeleteStoredProcedureAsync(sproc.SelfLink);
				}

				// Delete all user defined functions
				Console.WriteLine("Deleting all user defined functions...");
				IEnumerable<UserDefinedFunction> udfs = client.CreateUserDefinedFunctionQuery(collectionUri).AsEnumerable();

				foreach (UserDefinedFunction udf in udfs)
				{
					await client.DeleteUserDefinedFunctionAsync(udf.SelfLink);
				}

				// Delete all triggers
				Console.WriteLine("Deleting all triggers...");
				IEnumerable<Trigger> triggers = client.CreateTriggerQuery(collectionUri).AsEnumerable();

				foreach (Trigger trigger in triggers)
				{
					await client.DeleteTriggerAsync(trigger.SelfLink);
				}

				// Delete all users
				Console.WriteLine("Deleting all users...");
				Uri databaseUri = UriFactory.CreateDatabaseUri("mydb");
				IEnumerable<User> users = client.CreateUserQuery(databaseUri).AsEnumerable();

				foreach (User user in users)
				{
					await client.DeleteUserAsync(user.SelfLink);
				}
			}
		}
	}
}
