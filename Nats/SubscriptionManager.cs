using System.Collections.Concurrent;
using System.Threading.Channels;
using Messaging.Routing;
using Microsoft.Extensions.Hosting;

namespace Messaging.Nats;

internal sealed class SubscriptionManager(
    IOptions<ConsumerOptions> options,
    IActionDescriptorCollectionProvider descriptorProvider,
    NatsStreamProvisioner provisioner,
    NatsMessageDispatcher dispatcher,
    ILogger<SubscriptionManager> logger) : BackgroundService
{
    private static readonly UnboundedChannelOptions BatchChannelOptions = new() { SingleReader = true, SingleWriter = true };

    private readonly ConsumerOptions _options = options.Value;

    private string[] _subjects = [];
    private int _maxDeliver;

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _subjects = [.. descriptorProvider.Router.Subjects.Select(_options.QualifySubject)];
        _maxDeliver = descriptorProvider.Descriptors.Items.Aggregate(
            _options.MaxDeliver,
            (max, descriptor) => Math.Max(max, descriptor.Retry.MaxDeliver));

        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_subjects.Length == 0)
        {
            logger.LogWarning("subscription_no_routes");
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SubscribeAsync(_subjects, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "subscription_loop_failed error={ErrorType}", exception.GetType().Name);
            }

            try
            {
                await Task.Delay(_options.ReconnectDelay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task SubscribeAsync(string[] subjects, CancellationToken cancellationToken)
    {
        INatsJSConsumer consumer = await provisioner.BindAsync(subjects, _maxDeliver, cancellationToken);
        NatsJSFetchOpts fetchOptions = new() { MaxMsgs = _options.PrefetchCount };

        while (!cancellationToken.IsCancellationRequested)
        {
            await ProcessBatchAsync(consumer, fetchOptions, cancellationToken);
        }
    }

    private async Task ProcessBatchAsync(INatsJSConsumer consumer, NatsJSFetchOpts fetchOptions, CancellationToken cancellationToken)
    {
        Channel<INatsJSMsg<byte[]>> received = Channel.CreateUnbounded<INatsJSMsg<byte[]>>(BatchChannelOptions);
        ConcurrentDictionary<INatsJSMsg<byte[]>, bool> unfinished = new(ReferenceEqualityComparer.Instance);

        using SemaphoreSlim gate = new(1, 1);
        using CancellationTokenSource batch = new();
        using CancellationTokenSource fetching = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, batch.Token);

        Task fetch = ReceiveAsync(consumer, fetchOptions, received.Writer, unfinished, fetching.Token);
        Task heartbeat = KeepAliveAsync(unfinished, gate, batch.Token);

        try
        {
            await foreach (INatsJSMsg<byte[]> message in received.Reader.ReadAllAsync(cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                MessageDisposition disposition = await dispatcher.DispatchAsync(message, cancellationToken);

                await SettleAsync(message, disposition, unfinished, gate);
            }
        }
        finally
        {
            await batch.CancelAsync();

            try
            {
                await Task.WhenAll(fetch, heartbeat);
            }
            finally
            {
                await ReleaseAsync(unfinished, gate);
            }
        }
    }

    private async Task ReleaseAsync(ConcurrentDictionary<INatsJSMsg<byte[]>, bool> unfinished, SemaphoreSlim gate)
    {
        if (unfinished.IsEmpty)
        {
            return;
        }

        logger.LogInformation("batch_released count={Count}", unfinished.Count);

        using CancellationTokenSource release = new(_options.ReconnectDelay);

        foreach (INatsJSMsg<byte[]> message in unfinished.Keys)
        {
            if (release.IsCancellationRequested)
            {
                logger.LogWarning("batch_release_timeout remaining={Remaining}", unfinished.Count);
                return;
            }

            await SettleAsync(message, MessageDisposition.Redeliver(TimeSpan.Zero), unfinished, gate, release.Token);
        }
    }

    private static async Task ReceiveAsync(
        INatsJSConsumer consumer,
        NatsJSFetchOpts fetchOptions,
        ChannelWriter<INatsJSMsg<byte[]>> writer,
        ConcurrentDictionary<INatsJSMsg<byte[]>, bool> unfinished,
        CancellationToken cancellationToken)
    {
        Exception? failure = null;

        try
        {
            await foreach (INatsJSMsg<byte[]> message in consumer.FetchAsync<byte[]>(fetchOptions, cancellationToken: cancellationToken))
            {
                _ = unfinished.TryAdd(message, true);
                _ = writer.TryWrite(message);
            }
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            _ = writer.TryComplete(failure);
        }
    }

    private async Task KeepAliveAsync(
        ConcurrentDictionary<INatsJSMsg<byte[]>, bool> unfinished,
        SemaphoreSlim gate,
        CancellationToken cancellationToken)
    {
        using PeriodicTimer timer = new(_options.AckWait / 2);

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                foreach (INatsJSMsg<byte[]> message in unfinished.Keys)
                {
                    await gate.WaitAsync(cancellationToken);

                    try
                    {
                        if (unfinished.ContainsKey(message))
                        {
                            await ExtendAsync(message, cancellationToken);
                        }
                    }
                    finally
                    {
                        _ = gate.Release();
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task ExtendAsync(INatsJSMsg<byte[]> message, CancellationToken cancellationToken)
    {
        try
        {
            await message.AckProgressAsync(cancellationToken: cancellationToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            logger.LogWarning(exception, "message_progress_failed subject={Subject}", message.Subject);
        }
    }

    private async Task SettleAsync(
        INatsJSMsg<byte[]> message,
        MessageDisposition disposition,
        ConcurrentDictionary<INatsJSMsg<byte[]>, bool> unfinished,
        SemaphoreSlim gate,
        CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(CancellationToken.None);

        try
        {
            _ = unfinished.TryRemove(message, out _);

            switch (disposition.Kind)
            {
                case MessageDispositionKind.Acknowledge:
                    await message.AckAsync(cancellationToken: cancellationToken);
                    break;
                case MessageDispositionKind.Redeliver:
                    await message.NakAsync(
                        new AckOpts { NakDelay = disposition.RedeliveryDelay > TimeSpan.Zero ? disposition.RedeliveryDelay : null },
                        cancellationToken);
                    break;
                case MessageDispositionKind.Terminate:
                    await message.AckTerminateAsync(cancellationToken: cancellationToken);
                    break;
            }
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "message_settle_failed subject={Subject} disposition={Disposition}",
                message.Subject,
                disposition.Kind);
        }
        finally
        {
            _ = gate.Release();
        }
    }
}
