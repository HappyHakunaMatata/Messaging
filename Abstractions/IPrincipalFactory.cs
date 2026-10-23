namespace Messaging.Abstractions;

public interface IPrincipalFactory
{
    ClaimsPrincipal Create(IReadOnlyDictionary<string, string> headers);
}
