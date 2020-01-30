using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobTriggerFunction
{
    public static class BlobFunction
    {
        private static ServiceClient _sServiceClient;
        private static readonly string ServiceConnectionString = Environment.GetEnvironmentVariable("ServiceConnectionString");
        private static readonly string StorageAccountName = Environment.GetEnvironmentVariable("StorageAccountName");
        private static readonly string StorageAccessKey = Environment.GetEnvironmentVariable("StorageAccessKey");
        private static readonly string EndPointUriPath = Environment.GetEnvironmentVariable("EndPointUriPath");
        private static readonly string DeviceMethod = Environment.GetEnvironmentVariable("DeviceMethod");
        private static readonly string DeviceId = Environment.GetEnvironmentVariable("DeviceId");

        [FunctionName("TriggerFunction")]
        public static async Task Run([BlobTrigger("<folder_name>/{name}", Connection = "AzureWebJobsStorage")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            
            string content;

            using (StreamReader sr = new StreamReader(myBlob, Encoding.UTF8))
            {
                content = sr.ReadToEnd().Replace(Environment.NewLine, ",").Replace(@"\", "");
            }

            string jsonMessage = GetJsonFormatObjectMessage(content);
            
            CloudToDeviceMethod cloudToDeviceMethod = new CloudToDeviceMethod(DeviceMethod)
            {
                ResponseTimeout = TimeSpan.FromSeconds(30)
            };

            cloudToDeviceMethod.SetPayloadJson(jsonMessage);

            try
            {
                // Create a ServiceClient to communicate with service-facing endpoint on your hub.
                _sServiceClient = ServiceClient.CreateFromConnectionString(ServiceConnectionString);
                CloudToDeviceMethodResult response = await _sServiceClient.InvokeDeviceMethodAsync(DeviceId, cloudToDeviceMethod);
                string json = response.GetPayloadAsJson();
                log.LogInformation($"Result '{json}' '{response.Status}'");

                // deletes blob file after data have been sent to device
                await DeleteBlobFile(name);
                log.LogInformation($"Deleted Blob: {name}"); 
            }
            catch (StorageException e)
            {
                log.LogError(e.Message);
                log.LogError(e.StackTrace);
            }
            catch (Exception e)
            {
                log.LogError(e.Message);
                log.LogError(e.StackTrace);
            }
        }

        private static async Task DeleteBlobFile(string name)
        {
            Uri packageUri = new Uri($"{EndPointUriPath}/{name}");
            StorageCredentials credentials = new StorageCredentials(StorageAccountName, StorageAccessKey);
            CloudBlockBlob b = new CloudBlockBlob(packageUri, credentials);
            
            try
            {
                await b.DeleteIfExistsAsync();
            }
            catch
            {
                throw new StorageException("Blob not deleted...");
            }
        }

        private static string GetJsonFormatObjectMessage(string content)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{ <key>: [");
            sb.Append(content);
            sb.Append("]}");

            return sb.ToString();
        }
    }
}
