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
    private readonly ConsumerOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SubscribeAsync(stoppingToken);
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
        NatsJSConsumeOpts consumeOptions = new() { MaxMsgs = _options.PrefetchCount };

        await foreach (INatsJSMsg<byte[]> message in consumer.ConsumeAsync<byte[]>(opts: consumeOptions, cancellationToken: cancellationToken))
        {
            await dispatcher.DispatchAsync(message, cancellationToken);
        }
    }
}
