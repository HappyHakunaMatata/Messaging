namespace Messaging.Abstractions;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class ActionAttribute(string action) : Attribute
{
    public string Action { get; } = action;
}
