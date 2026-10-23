namespace Messaging.Abstractions;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class RetryAttribute : Attribute
{
    public int MaxDeliver { get; set; }

    public int[] BackoffSeconds { get; set; } = [];
}
