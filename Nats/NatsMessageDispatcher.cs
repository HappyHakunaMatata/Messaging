using Messaging.Infrastructure;
using Messaging.Routing;

namespace Messaging.Nats;

internal sealed class NatsMessageDispatcher(
    IOptions<ConsumerOptions> options,
    IActionDescriptorCollectionProvider descriptorProvider,
    IServiceScopeFactory scopeFactory,
    IPrincipalFactory principalFactory,
    ILogger<NatsMessageDispatcher> logger)
{
    private readonly ConsumerOptions _options = options.Value;

    public async Task DispatchAsync(INatsJSMsg<byte[]> message, CancellationToken cancellationToken)
    {
        string subject = message.Subject[_options.Prefix.Length..];
        IReadOnlyDictionary<string, string> headers = NatsHeaderCollectionFactory.Create(message.Headers);
        _ = headers.TryGetValue(NatsHeaderNames.Action, out string? action);

        if (!descriptorProvider.Router.TryMatch(subject, action, out NatsEndpoint? endpoint))
        {
            logger.LogWarning("message_unroutable subject={Subject} action={Action}", subject, action);
            await AcknowledgeAsync(message);
            return;
        }

        try
        {
            await InvokeAsync(endpoint, subject, headers, message, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogInformation(
                "message_cancelled subject={Subject} handler={Handler} num_delivered={NumDelivered}",
                message.Subject,
                endpoint.DisplayName,
                message.Metadata?.NumDelivered);

            // Not immediate: an immediate NAK is redelivered into this instance's own pull request before its UNSUB
            // is processed, and that copy is dropped with the subscription. The first backoff step is enough.
            await RedeliverAsync(message, _options.RedeliveryDelay(1));
            return;
        }
        catch (NatsMessageRejectedException rejection)
        {
            logger.LogError(
                rejection,
                "message_rejected subject={Subject} handler={Handler} error={ErrorType}",
                message.Subject,
                endpoint.DisplayName,
                rejection.GetType().Name);

            await TerminateAsync(message);
            return;
        }
        catch (Exception exception)
        {
            await FailAsync(message, endpoint, exception);
            return;
        }

        await AcknowledgeAsync(message);
    }

    private async Task InvokeAsync(
        NatsEndpoint endpoint,
        string subject,
        IReadOnlyDictionary<string, string> headers,
        INatsJSMsg<byte[]> message,
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

        await NatsActionInvokerFactory.CreateInvoker(context, endpoint).InvokeAsync();
    }

    private async Task FailAsync(INatsJSMsg<byte[]> message, NatsEndpoint endpoint, Exception exception)
    {
        NatsJSMsgMetadata? metadata = message.Metadata;

        if (metadata is null || metadata.Value.NumDelivered < (ulong)_options.MaxDeliver)
        {
            TimeSpan delay = _options.RedeliveryDelay(metadata?.NumDelivered ?? 1);

            logger.LogError(
                exception,
                "message_failed subject={Subject} handler={Handler} num_delivered={NumDelivered} redelivery_delay={RedeliveryDelay} error={ErrorType}",
                message.Subject,
                endpoint.DisplayName,
                metadata?.NumDelivered,
                delay,
                exception.GetType().Name);

            await RedeliverAsync(message, delay);
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

        await TerminateAsync(message);
    }

    // Acknowledgements deliberately ignore the stop token: once the handler has finished, the ack has to reach
    // the server even during shutdown, otherwise the message comes back and is processed a second time.
    private async Task AcknowledgeAsync(INatsJSMsg<byte[]> message)
    {
        try
        {
            await message.AckAsync(cancellationToken: CancellationToken.None);
        }
        catch (Exception failure)
        {
            logger.LogWarning(failure, "message_ack_failed subject={Subject}", message.Subject);
        }
    }

    private async Task RedeliverAsync(INatsJSMsg<byte[]> message, TimeSpan delay)
    {
        try
        {
            await message.NakAsync(
                new AckOpts { NakDelay = delay > TimeSpan.Zero ? delay : null },
                CancellationToken.None);
        }
        catch (Exception failure)
        {
            logger.LogWarning(failure, "message_nak_failed subject={Subject}", message.Subject);
        }
    }

    private async Task TerminateAsync(INatsJSMsg<byte[]> message)
    {
        try
        {
            await message.AckTerminateAsync(cancellationToken: CancellationToken.None);
        }
        catch (Exception failure)
        {
            logger.LogWarning(failure, "message_terminate_failed subject={Subject}", message.Subject);
        }
    }
}
