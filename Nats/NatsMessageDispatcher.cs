using Messaging.Infrastructure;
using Messaging.Routing;

namespace Messaging.Nats;

internal sealed class NatsMessageDispatcher(
    IOptions<ConsumerOptions> options,
    IActionDescriptorCollectionProvider descriptorProvider,
    IServiceScopeFactory scopeFactory,
    ILogger<NatsMessageDispatcher> logger)
{
    private readonly ConsumerOptions _options = options.Value;

    public async Task<MessageDisposition> DispatchAsync(INatsJSMsg<byte[]> message, CancellationToken cancellationToken)
    {
        string subject = message.Subject[_options.Prefix.Length..];
        IReadOnlyDictionary<string, string> headers = NatsHeaderCollectionFactory.Create(message.Headers);
        _ = headers.TryGetValue(NatsHeaderNames.Action, out string? action);

        if (!descriptorProvider.Router.TryMatch(subject, action, out NatsEndpoint? endpoint))
        {
            logger.LogWarning("message_unroutable subject={Subject} action={Action}", subject, action);
            return MessageDisposition.Acknowledge;
        }

        using CancellationTokenSource timeout = new(_options.MaxProcessingTime);
        using CancellationTokenSource handlerCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);

        Task invocation = InvokeAsync(endpoint, subject, headers, message, handlerCancellation.Token);
        Task deadline = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);

        if (await Task.WhenAny(invocation, deadline) == deadline && !invocation.IsCompleted)
        {
            Abandon(invocation, message, endpoint);

            return Fail(message, endpoint, new TimeoutException($"Handler exceeded {_options.MaxProcessingTime}."), _options.MaxProcessingTime);
        }

        try
        {
            await invocation;
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            return Fail(message, endpoint, new TimeoutException($"Handler exceeded {_options.MaxProcessingTime}."));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogInformation(
                "message_cancelled subject={Subject} handler={Handler} num_delivered={NumDelivered}",
                message.Subject,
                endpoint.DisplayName,
                message.Metadata?.NumDelivered);

            return MessageDisposition.Redeliver(TimeSpan.Zero);
        }
        catch (NatsMessageRejectedException rejection)
        {
            logger.LogError(
                rejection,
                "message_rejected subject={Subject} handler={Handler} error={ErrorType}",
                message.Subject,
                endpoint.DisplayName,
                rejection.GetType().Name);

            return MessageDisposition.Terminate;
        }
        catch (Exception exception)
        {
            return Fail(message, endpoint, exception);
        }

        return MessageDisposition.Acknowledge;
    }

    private async Task InvokeAsync(
        NatsEndpoint endpoint,
        string subject,
        IReadOnlyDictionary<string, string> headers,
        INatsJSMsg<byte[]> message,
        CancellationToken cancellationToken)
    {
        await using AsyncServiceScope scope = scopeFactory.CreateAsyncScope();

        DefaultNatsContext context = new(
            subject,
            headers,
            message.Data ?? ReadOnlyMemory<byte>.Empty,
            scope.ServiceProvider,
            endpoint.Descriptor,
            cancellationToken);

        scope.ServiceProvider.GetRequiredService<NatsContextAccessor>().Context = context;

        await NatsActionInvokerFactory.CreateInvoker(context, endpoint).InvokeAsync();
    }

    private void Abandon(Task invocation, INatsJSMsg<byte[]> message, NatsEndpoint endpoint)
    {
        _ = invocation.ContinueWith(
            completed => logger.LogWarning(
                completed.Exception?.GetBaseException(),
                "message_handler_overran subject={Subject} handler={Handler} num_delivered={NumDelivered} status={Status}",
                message.Subject,
                endpoint.DisplayName,
                message.Metadata?.NumDelivered,
                completed.Status),
            CancellationToken.None,
            TaskContinuationOptions.NotOnCanceled | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private MessageDisposition Fail(INatsJSMsg<byte[]> message, NatsEndpoint endpoint, Exception exception, TimeSpan minimumDelay = default)
    {
        NatsJSMsgMetadata? metadata = message.Metadata;
        RetryPolicy retry = endpoint.Descriptor.Retry;

        if (metadata is null || metadata.Value.NumDelivered < (ulong)retry.MaxDeliver)
        {
            TimeSpan delay = TimeSpan.FromTicks(Math.Max(retry.RedeliveryDelay(metadata?.NumDelivered ?? 1).Ticks, minimumDelay.Ticks));

            logger.LogError(
                exception,
                "message_failed subject={Subject} handler={Handler} num_delivered={NumDelivered} redelivery_delay={RedeliveryDelay} error={ErrorType}",
                message.Subject,
                endpoint.DisplayName,
                metadata?.NumDelivered,
                delay,
                exception.GetType().Name);

            return MessageDisposition.Redeliver(delay);
        }

        logger.LogError(
            exception,
            "message_abandoned subject={Subject} handler={Handler} num_delivered={NumDelivered} max_deliver={MaxDeliver} error={ErrorType}",
            message.Subject,
            endpoint.DisplayName,
            metadata.Value.NumDelivered,
            retry.MaxDeliver,
            exception.GetType().Name);

        return MessageDisposition.Terminate;
    }
}
