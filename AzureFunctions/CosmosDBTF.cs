using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace CosmosDBTriggerFunction
{
    public static class CosmosDBTF
    {
        private static readonly string ServiceConnectionString = Environment.GetEnvironmentVariable("ServiceConnectionString");
        private static readonly string DeviceMethod = Environment.GetEnvironmentVariable("DeviceMethod");
        private static readonly string DeviceId = Environment.GetEnvironmentVariable("DeviceId");
        private static ServiceClient _sServiceClient;

        [FunctionName("TriggerFunction")]
        public static async Task Run([CosmosDBTrigger("<database_name>", "<collection_name>", ConnectionStringSetting = "ConnetionString", LeaseCollectionName = "AnomalyLeases", CreateLeaseCollectionIfNotExists = true )]IReadOnlyList<Document> input, ILogger log)
        {
            if (input == null || input.Count <= 0) return;

            Initialize();

            log.LogInformation($"Documents modified: {input.Count}");
            log.LogInformation($"First document Id: {input[0].Id}");

            string jsonMessage = GetJsonFormatObjectMessage(input);
            log.LogInformation(jsonMessage);

            try
            {
                CloudToDeviceMethodResult response = await SendMessageToDevice(jsonMessage);
                log.LogInformation($"result '{response.GetPayloadAsJson()}' status: '{response.Status}'");
            }
            catch (Exception e)
            {
                log.LogInformation(e.Message);
            }
        }

        private static void Initialize()
        {
            // Create a ServiceClient to communicate with service-facing endpoint on your hub.
            _sServiceClient = ServiceClient.CreateFromConnectionString(ServiceConnectionString);
        }

        private static async Task<CloudToDeviceMethodResult> SendMessageToDevice(string jsonMessage)
        {
            CloudToDeviceMethod cloudToDeviceMethod = new CloudToDeviceMethod(DeviceMethod)
            {
                ResponseTimeout = TimeSpan.FromSeconds(30)
            };

            cloudToDeviceMethod.SetPayloadJson(jsonMessage);

            try
            {
                return await _sServiceClient.InvokeDeviceMethodAsync(DeviceId, cloudToDeviceMethod);
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
            
        }
        
        private static string GetJsonFormatObjectMessage(IReadOnlyCollection<Document> input)
        {
            uint n = 0;
            uint m = (uint) (input.Count() - 1);

            StringBuilder sb = new StringBuilder();
            sb.Append("{ <key>: [");
            foreach (Document document in input)
            {
                ParseMessage(out string parsedDocs, document.ToString());
                sb.Append(parsedDocs);
                
                if (n < m)
                {
                    sb.Append(","); 
                }
                n++;
            }
            sb.Append("]}");

            return sb.ToString();
        }

        private static void ParseMessage(out string parsedDoc, string toString)
        {
            TypeMessage am = new TypeMessage(toString);
            parsedDoc = am.ToString();
        }

        private class TypeMessage
        {
            private readonly long _date;
            private readonly string _name;
            private readonly string _type;
            private readonly float _value;
            private readonly float _min;
            private readonly float _max;
            private readonly ushort _boolValue;

            public TypeMessage(string json)
            {
                Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
                JObject jo = JObject.Parse(json);
                _date = Convert.ToInt64(jo["date"]);
                _name = jo["name"].ToString();
                _type = jo["type"].ToString();
                _value = (float) jo["value"];
                _min = (float)jo["min"];
                _max = (float)(jo["max"]);
                _boolValue = Convert.ToUInt16(jo["boolValue"]);
            }

            public override string ToString()
            {
                string jsonMsg = $"{{date: {_date},name: \"{_name}\",type: \"{_type}\",value: {_value},min: {_min},max: {_max},boolValue: {_boolValue}}}";
                JObject jo = JObject.Parse(jsonMsg);
                return jo.ToString();
            }
        }
    }
}
