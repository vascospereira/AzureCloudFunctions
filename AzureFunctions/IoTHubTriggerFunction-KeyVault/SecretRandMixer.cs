using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventHubs;
using System.Text;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using Azure;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;

namespace Mi60ghzCloudFun
{
    public static class SecretRandMixer
    {
        private static readonly string KeyVaultName = Environment.GetEnvironmentVariable("KEY_VAULT_NAME");

        [FunctionName("FunctionSecretRandMixer")]
        public static void Run([IoTHubTrigger("messages/events", Connection = "ConnectionString")]EventData message, ILogger log)
        {
            log.LogInformation($"C# IoT Hub trigger function processed a message: {Encoding.UTF8.GetString(message.Body.Array)}");
            JObject jo = JObject.Parse(Encoding.UTF8.GetString(message.Body.Array));
            
            var kvUri = $"https://{KeyVaultName}.vault.azure.net";
            
            //log.LogInformation(kvUri);
            SecretClient client = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());
            
            try
            {
                string secretName = jo.SelectToken("tagId").ToString();
                // get the secret
                KeyVaultSecret secret = client.GetSecret(secretName);
                log.LogInformation($"Your secret is '{secret.Value}'.");
                string date = DateTime.Today.ToShortDateString();
                string firstRandNum = jo.SelectToken("rand1").ToString();
                string secondRandNum = jo.SelectToken("rand2").ToString();

                // lets create a super key... 
                string superKey = $"{date}.{secondRandNum}-{secret.Value}-{firstRandNum}-keepsafedrinkRADEBERGER.";
                log.LogInformation($"what a super key ---> [{superKey}] <--- ok, maybe not...");
            }
            catch(RequestFailedException ex)
            {
                log.LogInformation("secret not found... missing 'secret name' or 'secret value'");
                log.LogInformation(ex.Message);
            }

        }
    }
}