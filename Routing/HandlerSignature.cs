namespace Messaging.Routing;

internal sealed record HandlerSignature(ParameterDescriptor[] Parameters, Type? MessageType)
{
    public static HandlerSignature Describe(string displayName, MethodInfo method, IServiceProviderIsService? isService)
    {
        ParameterInfo[] parameters = method.GetParameters();
        ParameterDescriptor[] descriptors = new ParameterDescriptor[parameters.Length];
        Type? messageType = null;

        for (int index = 0; index < parameters.Length; index++)
        {
            ParameterInfo parameter = parameters[index];

            if (parameter.ParameterType.IsByRef)
            {
                throw new InvalidOperationException(
                    $"Handler '{displayName}' declares by-reference parameter '{parameter.Name}'. " +
                    "Handler parameters must be passed by value.");
            }

            NatsBindingSource source = Resolve(displayName, parameter, isService, ref messageType);

            descriptors[index] = new ParameterDescriptor
            {
                Name = parameter.Name ?? index.ToString(CultureInfo.InvariantCulture),
                ParameterType = parameter.ParameterType,
                BindingSource = source
            };
        }

        return new HandlerSignature(descriptors, messageType);
    }

    private static NatsBindingSource Resolve(
        string displayName,
        ParameterInfo parameter,
        IServiceProviderIsService? isService,
        ref Type? messageType)
    {
        Type parameterType = parameter.ParameterType;

        if (typeof(NatsContext).IsAssignableFrom(parameterType))
        {
            return NatsBindingSource.Context;
        }

        if (parameterType == typeof(CancellationToken))
        {
            return NatsBindingSource.CancellationToken;
        }

        bool fromServices = parameter.IsDefined(typeof(FromServicesAttribute), inherit: true);

        if (!fromServices && messageType is null && isService?.IsService(parameterType) != true)
        {
            messageType = parameterType;
            return NatsBindingSource.Message;
        }

        if (isService?.IsService(parameterType) == false)
        {
            throw new InvalidOperationException(
                $"Handler '{displayName}' takes parameter '{parameter.Name}' of type '{parameterType}', which is " +
                $"neither the message payload nor a registered service. A handler binds at most one payload " +
                $"parameter; every other parameter is resolved from dependency injection.");
        }

        return NatsBindingSource.Services;
    }
}
