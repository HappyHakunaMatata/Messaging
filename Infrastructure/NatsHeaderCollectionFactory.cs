using System.Collections.Frozen;
using Microsoft.Extensions.Primitives;

namespace Messaging.Infrastructure;

internal sealed class NatsHeaderCollectionFactory
{
    private static readonly FrozenDictionary<string, string> _empty = FrozenDictionary<string, string>.Empty;

    public IReadOnlyDictionary<string, string> Create(NatsHeaders? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return _empty;
        }

        Dictionary<string, string> result = new(headers.Count, StringComparer.OrdinalIgnoreCase);

        foreach (KeyValuePair<string, StringValues> header in headers)
        {
            result[header.Key] = header.Value.ToString();
        }

        return result;
    }
}
