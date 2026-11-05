using System.Collections.Frozen;
using Messaging.Infrastructure;

namespace Messaging.Routing;

internal sealed class EndpointRouterFactory(NatsEndpointFactory endpointFactory)
{
    public EndpointRouter Create(IReadOnlyList<NatsActionDescriptor> descriptors)
    {
        Dictionary<string, NatsEndpoint> bySubject = new(StringComparer.Ordinal);
        Dictionary<string, Dictionary<string, NatsEndpoint>> byClassSubject = new(StringComparer.Ordinal);

        for (int index = 0; index < descriptors.Count; index++)
        {
            NatsActionDescriptor descriptor = descriptors[index];
            NatsEndpoint endpoint = endpointFactory.Create(descriptor);

            if (bySubject.TryGetValue(descriptor.Subject, out NatsEndpoint? claimed))
            {
                throw Ambiguous(descriptor.Subject, claimed.DisplayName, descriptor.DisplayName);
            }

            bySubject.Add(descriptor.Subject, endpoint);

            if (descriptor.ClassSubject is null || descriptor.Action is null)
            {
                continue;
            }

            if (!byClassSubject.TryGetValue(descriptor.ClassSubject, out Dictionary<string, NatsEndpoint>? actions))
            {
                actions = new Dictionary<string, NatsEndpoint>(StringComparer.Ordinal);
                byClassSubject.Add(descriptor.ClassSubject, actions);
            }

            if (actions.TryGetValue(descriptor.Action, out NatsEndpoint? claimedAction))
            {
                throw Ambiguous(
                    $"{descriptor.ClassSubject} ({NatsHeaderNames.Action}: {descriptor.Action})",
                    claimedAction.DisplayName,
                    descriptor.DisplayName);
            }

            actions.Add(descriptor.Action, endpoint);
        }

        return new EndpointRouter(
            bySubject.ToFrozenDictionary(StringComparer.Ordinal),
            byClassSubject.ToFrozenDictionary(
                entry => entry.Key,
                entry => entry.Value.ToFrozenDictionary(StringComparer.Ordinal),
                StringComparer.Ordinal),
            CollectSubjects([.. byClassSubject.Keys], [.. bySubject.Keys]));
    }

    private static string[] CollectSubjects(string[] classSubjects, string[] routedSubjects)
        => [.. classSubjects.Concat(routedSubjects).Distinct(StringComparer.Ordinal)];

    private static InvalidOperationException Ambiguous(string route, string first, string second)
        => new($"Ambiguous route '{route}' is claimed by both '{first}' and '{second}'.");
}
