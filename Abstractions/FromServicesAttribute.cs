namespace Messaging.Abstractions;

[AttributeUsage(AttributeTargets.Parameter, AllowMultiple = false, Inherited = true)]
public sealed class FromServicesAttribute : Attribute;
