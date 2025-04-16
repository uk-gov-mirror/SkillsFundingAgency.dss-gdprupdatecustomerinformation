using Newtonsoft.Json;

namespace NCS.DSS.DataUtility.Models
{
    public class ActionPlan
    {
        [JsonProperty("id")]
        public Guid? ActionPlanId { get; set; } = Guid.NewGuid();

        public Guid? CustomerId { get; set; }
    }
}
