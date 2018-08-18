using Newtonsoft.Json;

namespace CosmosDbDemo.Demos
{
    public class BulkDeleteResponse
    {
        [JsonProperty(PropertyName = "count")]
        public int Count { get; set; }

        [JsonProperty(PropertyName = "continuationFlag")]
        public bool ContinuationFlag { get; set; }
    }
}