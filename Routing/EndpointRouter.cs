using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace Messaging.Routing;

internal sealed class EndpointRouter(
    FrozenDictionary<string, NatsEndpoint> bySubject,
    FrozenDictionary<string, FrozenDictionary<string, NatsEndpoint>> byClassSubject,
    IReadOnlyList<string> subjects)
{
    public IReadOnlyList<string> Subjects { get; } = subjects;

    public int Count => bySubject.Count;

    public bool TryMatch(string subject, string? action, [NotNullWhen(true)] out NatsEndpoint? endpoint)
    {
        if (!string.IsNullOrEmpty(action) &&
            byClassSubject.TryGetValue(subject, out FrozenDictionary<string, NatsEndpoint>? actions) &&
            actions.TryGetValue(action, out endpoint))
        {
            return true;
        }

        return bySubject.TryGetValue(subject, out endpoint);
    }
}
