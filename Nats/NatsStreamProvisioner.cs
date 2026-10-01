namespace Messaging.Nats;

internal sealed class NatsStreamProvisioner(
    INatsConnectionProvider connection,
    IOptions<ConsumerOptions> options,
    ILogger<NatsStreamProvisioner> logger)
{
    private readonly ConsumerOptions _options = options.Value;

    public async Task<INatsJSConsumer> BindAsync(IReadOnlyList<string> subjects, CancellationToken cancellationToken)
    {
        INatsJSContext context = connection.Context;

        _ = await context.CreateOrUpdateStreamAsync(
            new StreamConfig(_options.QualifiedStreamName, [.. subjects])
            {
                Retention = StreamConfigRetention.Limits,
                DuplicateWindow = _options.DuplicateWindow
            },
            cancellationToken);

        INatsJSConsumer consumer = await context.CreateOrUpdateConsumerAsync(
            _options.QualifiedStreamName,
            new ConsumerConfig
            {
                Name = _options.QualifiedDurableName,
                DurableName = _options.QualifiedDurableName,
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                MaxDeliver = _options.MaxDeliver,
                Backoff = _options.Backoff,
                FilterSubjects = [.. subjects]
            },
            cancellationToken);

        logger.LogInformation(
            "consumers_bound stream={Stream} durable={Durable} subjects={Subjects}",
            _options.QualifiedStreamName,
            _options.QualifiedDurableName,
            string.Join(", ", subjects));

        return consumer;
    }
}
