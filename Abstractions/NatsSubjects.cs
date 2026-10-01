namespace Messaging.Abstractions;

public static class NatsSubjects
{
    public const string Separator = ".";

    public const string Wildcard = ">";

    public static string Compose(string subject, string action) => string.Concat(subject, Separator, action);
}
