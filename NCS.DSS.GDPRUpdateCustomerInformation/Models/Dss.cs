using Newtonsoft.Json;

namespace NCS.DSS.DataUtility.Models
{
    public class ActionPlan
    {
        [JsonProperty("id")]
        public Guid? ActionPlanId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Action
    {
        [JsonProperty("id")]
        public Guid? ActionId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Address
    {
        [JsonProperty("id")]
        public Guid? AddressId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class ContactDetail
    {
        [JsonProperty("id")]
        public Guid? ContactId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Customer
    {
        [JsonProperty("id")]
        public Guid? CustomerId { get; set; }
    }

    public class DiversityDetail
    {
        [JsonProperty("id")]
        public Guid? DiversityId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class EmploymentProgression
    {
        [JsonProperty("id")]
        public Guid? EmploymentProgressionId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Goal
    {
        [JsonProperty("id")]
        public Guid? GoalId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Interaction
    {
        [JsonProperty("id")]
        public Guid? InteractionId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class LearningProgression
    {
        [JsonProperty("id")]
        public Guid? LearningProgressionId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Outcome
    {
        [JsonProperty("id")]
        public Guid? OutcomeId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Session
    {
        [JsonProperty("id")]
        public Guid? SessionId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Subscription
    {
        [JsonProperty("id")]
        public Guid? SubscriptionId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Transfer
    {
        [JsonProperty("id")]
        public Guid? TransferId { get; set; }
        public Guid? CustomerId { get; set; }
    }

    public class Webchat
    {
        [JsonProperty("id")]
        public Guid? WebChatId { get; set; }
        public Guid? CustomerId { get; set; }
    }
}
