using Messaging.Infrastructure;
using Messaging.Routing;

namespace Messaging.Nats;

internal sealed class NatsMessageDispatcher(
    IOptions<ConsumerOptions> options,
    IActionDescriptorCollectionProvider descriptorProvider,
    IServiceScopeFactory scopeFactory,
    NatsActionInvokerFactory invokerFactory,
    NatsHeaderCollectionFactory headerFactory,
    IPrincipalFactory principalFactory,
    ILogger<NatsMessageDispatcher> logger)
{
    private readonly ConsumerOptions _options = options.Value;

    public async Task DispatchAsync(NatsJSMsg<byte[]> message, CancellationToken cancellationToken)
    {
        string subject = message.Subject[_options.Prefix.Length..];
        IReadOnlyDictionary<string, string> headers = headerFactory.Create(message.Headers);
        _ = headers.TryGetValue(NatsHeaderNames.Action, out string? action);

        if (!descriptorProvider.Router.TryMatch(subject, action, out NatsEndpoint? endpoint))
        {
            logger.LogWarning("message_unroutable subject={Subject} action={Action}", subject, action);
            await message.AckAsync(cancellationToken: cancellationToken);
            return;
        }

        try
        {
            await InvokeAsync(endpoint, subject, headers, message, cancellationToken);
            await message.AckAsync(cancellationToken: cancellationToken);
        }
        catch (NatsMessageRejectedException rejection)
        {
            logger.LogError(
                rejection,
                "message_rejected subject={Subject} handler={Handler} error={ErrorType}",
                message.Subject,
                endpoint.DisplayName,
                rejection.GetType().Name);

            await TerminateAsync(message, cancellationToken);
        }
        catch (Exception exception)
        {
            await FailAsync(message, endpoint, exception, cancellationToken);
        }
    }

    private async Task InvokeAsync(
        NatsEndpoint endpoint,
        string subject,
        IReadOnlyDictionary<string, string> headers,
        NatsJSMsg<byte[]> message,
        CancellationToken cancellationToken)
    {
        using IServiceScope scope = scopeFactory.CreateScope();

        DefaultNatsContext context = new(
            subject,
            headers,
            message.Data ?? ReadOnlyMemory<byte>.Empty,
            scope.ServiceProvider,
            endpoint.Descriptor,
            principalFactory,
            cancellationToken);

        scope.ServiceProvider.GetRequiredService<NatsContextAccessor>().Context = context;

        await invokerFactory.CreateInvoker(context, endpoint).InvokeAsync();
    }

    private async Task FailAsync(
        NatsJSMsg<byte[]> message,
        NatsEndpoint endpoint,
        Exception exception,
        CancellationToken cancellationToken)
    {
        NatsJSMsgMetadata? metadata = message.Metadata;

        if (metadata is null || metadata.Value.NumDelivered < (ulong)_options.MaxDeliver)
        {
            logger.LogError(
                exception,
                "message_failed subject={Subject} handler={Handler} num_delivered={NumDelivered} error={ErrorType}",
                message.Subject,
                endpoint.DisplayName,
                metadata?.NumDelivered,
                exception.GetType().Name);

            return;
        }

        logger.LogError(
            exception,
            "message_abandoned subject={Subject} handler={Handler} num_delivered={NumDelivered} max_deliver={MaxDeliver} error={ErrorType}",
            message.Subject,
            endpoint.DisplayName,
            metadata.Value.NumDelivered,
            _options.MaxDeliver,
            exception.GetType().Name);

        await TerminateAsync(message, cancellationToken);
    }

    private async Task TerminateAsync(NatsJSMsg<byte[]> message, CancellationToken cancellationToken)
    {
        try
        {
            await message.AckTerminateAsync(cancellationToken: cancellationToken);
        }
        catch (Exception failure)
        {
            logger.LogWarning(failure, "message_terminate_failed subject={Subject}", message.Subject);
        }
    }
}
