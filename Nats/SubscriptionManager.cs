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
        NatsJSConsumeOpts consumeOptions = new() { MaxMsgs = _options.PrefetchCount };

        await foreach (INatsJSMsg<byte[]> message in consumer.ConsumeAsync<byte[]>(opts: consumeOptions, cancellationToken: cancellationToken))
        {
            await dispatcher.DispatchAsync(message, cancellationToken);
        }
    }
}
