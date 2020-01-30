using System;
using System.Linq;
using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace FuncIoTCosmos
{
    public static class DeviceToCosmos
    {
        private static readonly string Endpoint = Environment.GetEnvironmentVariable("Endpoint");
        private static readonly string AuthKey = Environment.GetEnvironmentVariable("AuthKey");
        private static string _databaseId;
        private static string _collectionId;
        // Creating a single, static instance will yield greater performance, expecially in a high-throughput function
        private static readonly Lazy<DocumentClient> LazyClient = new Lazy<DocumentClient>(Initialize);
        private static DocumentClient Client => LazyClient.Value;

        [FunctionName("IoTDataCosmos")]
        public static async Task Run([IoTHubTrigger("messages_events", Connection = "ConnectionString")]EventData message, ILogger log)
        {
            log.LogInformation($"message from 'IoTHub': {Encoding.UTF8.GetString(message.Body.Array)}");
            if (Encoding.UTF8.GetString(message.Body.Array).Equals("{}"))
            {
                log.LogInformation("data message '\"{}\"' - any data to process. returning... ");
                return;
            }

            // Lazy start above
            //Initialize();

            JObject jo = JObject.Parse(Encoding.UTF8.GetString(message.Body.Array));
            log.LogInformation(Encoding.UTF8.GetString(message.Body.Array));
            
            log.LogInformation("setting database and collection...");
            SetDatabaseInfo(jo, log);
            log.LogInformation("database and collection set.");
            log.LogInformation("checking database...");
            await CheckDatabase();
            log.LogInformation("database OK.");
            
            log.LogInformation("sendind document...");
            SendMessageAsync(jo).Wait();
            log.LogInformation("document sent.");
            
        }

        private static async Task CheckDatabase()
        {
            await CreateDatabaseIfNotExistsAsync();
            await CreateCollectionIfNotExistsAsync();
        }

        private static void SetDatabaseInfo(JToken jo, ILogger log)
        {
            const string databaseName = "<database_name>"; //jo.SelectToken("data.databaseid").ToString();
            string topic = jo.SelectToken("<JSON_key_search:foo.bar").ToString();

            string[] parts = topic.Split("/");
            string collectionName = parts[0]; 
            log.LogInformation($"collection name: {collectionName}");

            _databaseId = databaseName.First().ToString().ToUpper() + databaseName.Substring(1);
            _collectionId = collectionName.First().ToString().ToUpper() + collectionName.Substring(1);
        }

        private static DocumentClient Initialize()
        {
            return new DocumentClient(new Uri(Endpoint), AuthKey);
        }

        private static async Task CreateCollectionIfNotExistsAsync()
        {
            try
            {
                await Client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId));
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    await Client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(_databaseId),
                        new DocumentCollection {Id = _collectionId});
                }
                else
                {
                    throw;
                }
            }
        }

        private static async Task CreateDatabaseIfNotExistsAsync()
        {
            try
            {
                await Client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(_databaseId));
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    await Client.CreateDatabaseAsync(new Database() {Id = _databaseId});
                }
                else
                {
                    throw;
                }
            }
        }

        private static async Task SendMessageAsync(JObject message)
        {
            await Client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId), message);
        }
    }
}