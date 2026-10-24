namespace Messaging.Nats;

internal sealed class NatsStreamProvisioner(
    INatsConnectionProvider connection,
    IOptions<ConsumerOptions> options)
{
    private readonly ConsumerOptions _options = options.Value;

    public async Task<INatsJSConsumer> BindAsync(IReadOnlyList<string> subjects, CancellationToken cancellationToken)
    {
        INatsJSContext context = connection.Context;

        _ = await context.CreateOrUpdateStreamAsync(
            new StreamConfig(_options.QualifiedStreamName, [.. subjects])
            {
                Retention = StreamConfigRetention.Workqueue,
                DuplicateWindow = _options.DuplicateWindow
            },
            cancellationToken);

        return await context.CreateOrUpdateConsumerAsync(
            _options.QualifiedStreamName,
            new ConsumerConfig
            {
                Name = _options.QualifiedDurableName,
                DurableName = _options.QualifiedDurableName,
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                AckWait = _options.AckWait,
                MaxDeliver = _options.MaxDeliver,
                FilterSubjects = [.. subjects]
            },
            cancellationToken);
    }
}
