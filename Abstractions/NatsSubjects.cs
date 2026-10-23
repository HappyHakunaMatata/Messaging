namespace Messaging.Abstractions;

public static class NatsSubjects
{
    public const string Separator = ".";

    public const string Wildcard = ">";

    public const string TokenWildcard = "*";

    public static string Compose(string subject, string action) => string.Concat(subject, Separator, action);

    internal static bool IsLiteral(string subject)
    {
        if (string.IsNullOrEmpty(subject))
        {
            return false;
        }

        foreach (string token in subject.Split(Separator))
        {
            if (token.Length == 0 ||
                token.Contains(Wildcard, StringComparison.Ordinal) ||
                token.Contains(TokenWildcard, StringComparison.Ordinal) ||
                token.Any(char.IsWhiteSpace))
            {
                return false;
            }
        }

        return true;
    }
}
