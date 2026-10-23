using Messaging.Routing;

namespace Messaging.Infrastructure;

internal sealed class ParameterBinder(
    IReadOnlyList<ParameterDescriptor> parameters,
    Func<ReadOnlyMemory<byte>, object?>? payloadDeserializer)
{
    public IDictionary<string, object?> Bind(NatsContext context)
    {
        Dictionary<string, object?> arguments = new(parameters.Count, StringComparer.Ordinal);

        for (int index = 0; index < parameters.Count; index++)
        {
            ParameterDescriptor parameter = parameters[index];

            arguments[parameter.Name] = Resolve(parameter, context);
        }

        return arguments;
    }

    public object?[] ToArguments(IDictionary<string, object?> bound)
    {
        if (parameters.Count == 0)
        {
            return [];
        }

        object?[] arguments = new object?[parameters.Count];

        for (int index = 0; index < arguments.Length; index++)
        {
            ParameterDescriptor parameter = parameters[index];

            arguments[index] = bound.TryGetValue(parameter.Name, out object? value)
                ? value
                : throw new InvalidOperationException($"No argument was bound for parameter '{parameter.Name}'.");
        }

        return arguments;
    }

    private object? Resolve(ParameterDescriptor parameter, NatsContext context) => parameter.BindingSource switch
    {
        NatsBindingSource.Context => context,
        NatsBindingSource.CancellationToken => context.CancellationToken,
        NatsBindingSource.Services => context.Services.GetRequiredService(parameter.ParameterType),
        NatsBindingSource.Message => Deserialize(parameter, context),
        _ => throw new InvalidOperationException(
            $"Parameter '{parameter.Name}' declares unsupported binding source '{parameter.BindingSource}'.")
    };

    private object? Deserialize(ParameterDescriptor parameter, NatsContext context)
        => payloadDeserializer is null
            ? throw new InvalidOperationException($"Parameter '{parameter.Name}' binds the payload but no deserializer was built for it.")
            : payloadDeserializer(context.Payload);
}
