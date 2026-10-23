namespace Messaging.Abstractions;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class SubjectAttribute(string subject) : Attribute
{
    public string Subject { get; } = subject;
}
