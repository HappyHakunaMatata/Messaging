using System.Linq.Expressions;
using System.Text.Json;

namespace Messaging.Infrastructure;

internal sealed class PayloadDeserializerFactory(IOptions<NatsOptions> options)
{
    private static readonly MethodInfo _definition = typeof(PayloadDeserializerFactory)
        .GetMethod(nameof(DeserializePayload), BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException($"'{nameof(DeserializePayload)}' was not found.");

    public Func<ReadOnlyMemory<byte>, object?> Create(Type messageType)
    {
        ParameterExpression payload = Expression.Parameter(typeof(ReadOnlyMemory<byte>), "payload");

        MethodCallExpression call = Expression.Call(
            _definition.MakeGenericMethod(messageType),
            payload,
            Expression.Constant(options.Value.SerializerOptions, typeof(JsonSerializerOptions)));

        return Expression
            .Lambda<Func<ReadOnlyMemory<byte>, object?>>(Expression.Convert(call, typeof(object)), payload)
            .Compile();
    }

    private static TMessage? DeserializePayload<TMessage>(ReadOnlyMemory<byte> payload, JsonSerializerOptions serializerOptions)
        => payload.IsEmpty ? default : JsonSerializer.Deserialize<TMessage>(payload.Span, serializerOptions);
}
