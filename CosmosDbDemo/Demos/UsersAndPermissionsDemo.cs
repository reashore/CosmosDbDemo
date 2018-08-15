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
	public static class UsersAndPermissionsDemo
	{
		public static Uri MyDbDatabaseUri => UriFactory.CreateDatabaseUri("mydb");

		public static async Task Run()
		{
			Debugger.Break();

			string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
			string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

			using (DocumentClient client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				ListUsers(client);

				User alice = await CreateUser(client, "Alice");
				User tom = await CreateUser(client, "Tom");
				ListUsers(client);

				ListPermissions(client, alice);
				ListPermissions(client, tom);

				const string sql = "SELECT VALUE c._self FROM c WHERE c.id = 'mystore'";
				string collectionSelfLink = client.CreateDocumentCollectionQuery(MyDbDatabaseUri, sql).AsEnumerable().First().Value;

				Permission alicePerm = await CreatePermission(client, alice, "AliceCollectionAccess", PermissionMode.All, collectionSelfLink);
				Permission tomPerm = await CreatePermission(client, tom, "TomCollectionAccess", PermissionMode.Read, collectionSelfLink);

				ListPermissions(client, alice);
				ListPermissions(client, tom);

				await TestPermissions(client, alice, collectionSelfLink);
				await TestPermissions(client, tom, collectionSelfLink);

				await DeletePermission(client, alice, alicePerm);
				await DeletePermission(client, tom, tomPerm);

				await DeleteUser(client, "Alice");
				await DeleteUser(client, "Tom");
			}
		}

		// Users

		private static void ListUsers(IDocumentClient client)
		{
			Console.WriteLine(">>> View Users in mydb <<<");

			List<User> users = client.CreateUserQuery(MyDbDatabaseUri).ToList();

			int i = 0;
			foreach (User user in users)
			{
				i++;
				Console.WriteLine();
				Console.WriteLine($" User #{i}");
				PrintUser(user);
			}

			Console.WriteLine();
			Console.WriteLine($"Total users in database mydb: {users.Count}");
		}

		private static void PrintUser(User user)
		{
			Console.WriteLine($"          User ID: {user.Id}");
			Console.WriteLine($"      Resource ID: {user.ResourceId}");
			Console.WriteLine($"        Self Link: {user.SelfLink}");
			Console.WriteLine($" Permissions Link: {user.PermissionsLink}");
			Console.WriteLine($"        Timestamp: {user.Timestamp}");
		}

		private static async Task<User> CreateUser(IDocumentClient client, string userId)
		{
			Console.WriteLine($">>> Create User {userId} <<<");

			User userDefinition = new User { Id = userId };
			ResourceResponse<User> result = await client.CreateUserAsync(MyDbDatabaseUri, userDefinition);
			User user = result.Resource;

			Console.WriteLine("Created new user");
			PrintUser(user);

			return user;
		}

		private static async Task DeleteUser(IDocumentClient client, string userId)
		{
			Console.WriteLine($">>> Delete User {userId} <<<");

			Uri userUri = UriFactory.CreateUserUri("mydb", userId);
			await client.DeleteUserAsync(userUri);

			Console.WriteLine($"Deleted user {userId}");
		}

		// Permissions

		private static void ListPermissions(IDocumentClient client, User user)
		{
			Console.WriteLine($">>> View Permissions for {user.Id} <<<");

			List<Permission> permissions = client.CreatePermissionQuery(user.PermissionsLink).ToList();

			int i = 0;
			foreach (Permission permission in permissions)
			{
				i++;
				Console.WriteLine();
				Console.WriteLine($"Permission #{i}");
				PrintPermission(permission);
			}

			Console.WriteLine($"Total permissions for {user.Id}: {permissions.Count}");
		}

		private static void PrintPermission(Permission perm)
		{
			Console.WriteLine($"    Permission ID: {perm.Id}");
			Console.WriteLine($"      Resource ID: {perm.ResourceId}");
			Console.WriteLine($"  Permission Mode: {perm.PermissionMode}");
			Console.WriteLine($"            Token: {perm.Token}");
			Console.WriteLine($"        Timestamp: {perm.Timestamp}");
		}

		private static async Task<Permission> CreatePermission(IDocumentClient client, User user, string permId, PermissionMode permissionMode, string resourceLink)
		{
			Console.WriteLine($">>> Create Permission {permId} for {user.Id} <<<");

			Permission permissionDefinition = new Permission { Id = permId, PermissionMode = permissionMode, ResourceLink = resourceLink };
			ResourceResponse<Permission> result = await client.CreatePermissionAsync(user.SelfLink, permissionDefinition);
			Permission permission = result.Resource;

			Console.WriteLine("Created new permission");
			PrintPermission(permission);

			return permission;
		}

	    // ReSharper disable once UnusedMember.Local
	    private static async Task DeletePermission(IDocumentClient client, User user, string permissionId)
        {
            Console.WriteLine($">>> Delete Permission {permissionId} from {user.Id} <<<");

            Uri permUri = UriFactory.CreatePermissionUri("mydb", "mystore", permissionId);
            await client.DeletePermissionAsync(permUri);

            Console.WriteLine("Deleted permission {permId} from user {user.Id}");
        }

        private static async Task DeletePermission(IDocumentClient client, User user, Permission permission)
		{
			Console.WriteLine($">>> Delete Permission {permission.Id} from {user.Id} <<<");

			await client.DeletePermissionAsync(permission.SelfLink);

			Console.WriteLine("Deleted permission {permId} from user {user.Id}");
		}

		private static async Task TestPermissions(IDocumentClient client, User user, string collectionLink)
		{
			Permission perm = client.CreatePermissionQuery(user.PermissionsLink).AsEnumerable().First(p => p.ResourceLink == collectionLink);
			string resourceToken = perm.Token;

			dynamic documentDefinition = new
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

			Console.WriteLine($"Trying to create & delete document as user {user.Id}");

			try
			{
				string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];

				using (DocumentClient restrictedClient = new DocumentClient(new Uri(endpoint), resourceToken))
				{
					dynamic document = await restrictedClient.CreateDocumentAsync(collectionLink, documentDefinition);
					Console.WriteLine($"Successfully created document: {document.Resource.id}");

					RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey("11229") };
					await restrictedClient.DeleteDocumentAsync(document.Resource._self, options);
					Console.WriteLine($"Successfully deleted document: {document.Resource.id}");
				}
			}
			catch (Exception exception)
			{
				Console.WriteLine($"ERROR: {exception.Message}");
			}
		}
	}
}
