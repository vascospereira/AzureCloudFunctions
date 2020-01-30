using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace CosmosDBManager
{
    internal static class Program
    {
        private const string EndpointUrl = "<URI>";
        private const string AuthorizationKey = "<KEY>";
        private const string DatabaseId = "<database_name>";
        private const string CollectionId = "<collection_name>";

        private static void Main()
        {
            try
            {
                WorkDocumentsAsync().Wait();
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine($"Error: {e.Message} {baseException.Message}");
            }

            Console.WriteLine();
            Console.WriteLine("Done");

            Console.ReadLine();
        }

        private static async Task WorkDocumentsAsync()
        {
            using (var client = new DocumentClient(new Uri(EndpointUrl), AuthorizationKey))
            {
                //await CreateDatabase(client);
                //await CreateCollectionIfNotExistsAsync(client);
                //await QueryDocuments(client);
                await DeleteDocuments(client);
                //await DeleteCollection(client);
                //await DeleteDatabase(client);

                GetDatabases(client);
            }
        }

        private static async Task DeleteDocuments(IDocumentClient client)
        {
            Console.WriteLine(">>> Delete Documents <<<");
            Console.WriteLine();
            Console.WriteLine("Quering for documents to be deleted");

            //const string sql = "SELECT VALUE c._self FROM c WHERE STARTSWITH(c.name, 'Vasco') = true";
            const string sql = "SELECT VALUE c._self FROM c";
            Uri collectionLink = UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId);
            
            List<string> documentLinks = client.CreateDocumentQuery<string>(collectionLink, sql,  new FeedOptions { EnableCrossPartitionQuery = true}).ToList();
            
            //new RequestOptions {PartitionKey = new PartitionKey("Vasco")}
            Console.WriteLine($"Found {documentLinks.Count} documents to be deleted");
            foreach (var documentLink in documentLinks)
            {
                await client.DeleteDocumentAsync(documentLink);
            }
            Console.WriteLine($"Deleted {documentLinks.Count} documents");
        }

        private static async Task QueryDocuments(IDocumentClient client)
        {
            Console.WriteLine("#### Query Documents (paged results) ####");
            Console.WriteLine();
            Console.WriteLine("Quering for selected documents:");

            const string sql = "SELECT * FROM c WHERE c.name = 'Vasco'";
            Uri collectionLink = UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId);
            IDocumentQuery<dynamic> query = client.CreateDocumentQuery(collectionLink, sql).AsDocumentQuery();

            while (query.HasMoreResults)
            {
                FeedResponse<dynamic> documents = await query.ExecuteNextAsync();
                foreach (var document in documents)
                {
                    Console.WriteLine($"\tId: {document.id}; Station: {document.station}; Type: {document.type}; Anomaly: {document.anomaly}");
                }
            }
        }

        private static void GetDatabases(IDocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("#### Get Databases List ####");

            var databases = client.CreateDatabaseQuery().ToList();

            foreach (var database in databases)
            {
                Console.WriteLine($" Database Id: {database.Id}; Rid: {database.ResourceId}");
            }

            Console.WriteLine();
            Console.WriteLine($"Total databases: {databases.Count}");
        }

        private static async Task CreateCollectionIfNotExistsAsync(IDocumentClient client)
        {
            try
            {
                await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId));
                Console.WriteLine($"Already exists collection {CollectionId}");
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    Console.WriteLine($"**** Creating collection: {CollectionId} in {DatabaseId} ****");

                    DocumentCollection collection = new DocumentCollection {Id = CollectionId};
                    
                    //collection.PartitionKey.Paths.Add("/station");

                    ResourceResponse<DocumentCollection> result =
                        await client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(DatabaseId), collection);
                    
                    Console.WriteLine($"Created collection {result.Resource.Id}");

                    ViewCollection(result.Resource);
                }
                else
                {
                    throw;
                }
            }
        }

        private static async Task DeleteDatabase(IDocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("#### Deleting Database ####");

            Database database = client.CreateDatabaseQuery().AsEnumerable().First();
            await client.DeleteDatabaseAsync(database.SelfLink);
            Console.WriteLine($"Database Id: {database.Id}; Resource Id: {database.ResourceId} deleted");
        }

        private static async Task CreateDatabase(IDocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("#### Creating Database ####");

            Database database = client.CreateDatabaseQuery().Where(db => db.Id == DatabaseId).ToArray()
                                    .FirstOrDefault() ?? await client.CreateDatabaseAsync(new Database() { Id = DatabaseId });

            Console.WriteLine($"Database Id: {database.Id}; Resource Id: {database.ResourceId}");
        }

        private static void ViewCollection(DocumentCollection collection)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Collection ID: {collection.Id}")
                .AppendLine($"Resource ID: {collection.ResourceId}")
                .AppendLine($"Self Link: {collection.SelfLink}")
                .AppendLine($"Documents Link: {collection.DocumentsLink}")
                .AppendLine($"UDFs Link: {collection.UserDefinedFunctionsLink}")
                .AppendLine($"StoredProcs Link: {collection.StoredProceduresLink}")
                .AppendLine($"Triggers Link: {collection.TriggersLink}")
                .AppendLine($"Timestamp: {collection.Timestamp}");

            Console.WriteLine(sb.ToString());
        }

        private static async Task DeleteCollection(IDocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine($"**** Delete Collection { CollectionId } in { DatabaseId } ****");

            var query = new SqlQuerySpec
            {
                QueryText = "SELECT * FROM c WHERE c.id = @id",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter { Name = "@id", Value = CollectionId }
                }
            };

            DocumentCollection collection = client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(DatabaseId), query).AsEnumerable().First();

            await client.DeleteDocumentCollectionAsync(collection.SelfLink);

            Console.WriteLine($"Deleted collection {collection.Id} from database {DatabaseId}");
        }
    }
}

