namespace Messaging.Security;

public sealed class HeaderPrincipalFactory : IPrincipalFactory
{
    public ClaimsPrincipal Create(IReadOnlyDictionary<string, string> headers)
    {
        ArgumentNullException.ThrowIfNull(headers);

        if (!headers.TryGetValue(NatsHeaderNames.UserId, out string? userId) ||
            string.IsNullOrWhiteSpace(userId))
        {
            return new ClaimsPrincipal(new ClaimsIdentity());
        }

        return new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim(ClaimTypes.NameIdentifier, userId)],
            NatsClaimTypes.AuthenticationType,
            ClaimTypes.Name,
            ClaimTypes.Role));
    }
}
