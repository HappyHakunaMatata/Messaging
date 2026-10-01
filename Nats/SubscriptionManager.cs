using Messaging.Routing;

namespace Messaging.Nats;

internal sealed class SubscriptionManager(
    IOptions<ConsumerOptions> options,
    IActionDescriptorCollectionProvider descriptorProvider,
    NatsStreamProvisioner provisioner,
    NatsMessageDispatcher dispatcher,
    ILogger<SubscriptionManager> logger) : IDisposable, IAsyncDisposable
{
    private readonly ConsumerOptions _options = options.Value;
    private readonly Lock _startGate = new();

    private CancellationTokenSource? _stopping;
    private Task? _loop;

    public void Start(CancellationToken applicationStopping)
    {
        lock (_startGate)
        {
            if (_loop is not null)
            {
                return;
            }

            _stopping = CancellationTokenSource.CreateLinkedTokenSource(applicationStopping);
            _loop = Task.Run(() => RunAsync(_stopping.Token), CancellationToken.None);
        }
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await SubscribeAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "subscription_loop_failed error={ErrorType}", exception.GetType().Name);
            }

            try
            {
                await Task.Delay(_options.ReconnectDelay, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task SubscribeAsync(CancellationToken cancellationToken)
    {
        EndpointRouter router = descriptorProvider.Router;
        string[] subjects = [.. router.Subjects.Select(_options.QualifySubject)];

        if (subjects.Length == 0)
        {
            logger.LogWarning("subscription_no_routes");
            return;
        }

        INatsJSConsumer consumer = await provisioner.BindAsync(subjects, cancellationToken);

        await foreach (NatsJSMsg<byte[]> message in consumer.ConsumeAsync<byte[]>(cancellationToken: cancellationToken))
        {
            await dispatcher.DispatchAsync(message, cancellationToken);
        }
    }

    public void Dispose()
    {
        DisposeCoreAsync(isAsync: false)
            .GetAwaiter()
            .GetResult();

        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeCoreAsync(isAsync: true)
            .ConfigureAwait(false);

        GC.SuppressFinalize(this);
    }

    private async ValueTask DisposeCoreAsync(bool isAsync)
    {
        var loop = Interlocked.Exchange(ref _loop, null);
        var stopping = Interlocked.Exchange(ref _stopping, null);

        if (loop is not null)
        {
            try
            {
                if (isAsync)
                    await loop.ConfigureAwait(false);
                else
                    loop.GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An exception occurred while disposing the loop.");
            }
            finally
            {
                loop.Dispose();
            }
        }

        if (stopping is not null)
        {
            if (isAsync)
                await stopping.CancelAsync().ConfigureAwait(false);
            else
                stopping.Cancel();

            stopping.Dispose();
        }
    }

}
