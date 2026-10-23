namespace Messaging.Nats;

internal sealed class NatsStreamProvisioner(
    INatsConnectionProvider connection,
    IOptions<ConsumerOptions> options,
    ILogger<NatsStreamProvisioner> logger)
{
    private const StreamConfigRetention Retention = StreamConfigRetention.Workqueue;
    private const int StreamNotFound = 404;

    private readonly ConsumerOptions _options = options.Value;

    public async Task<INatsJSConsumer> BindAsync(IReadOnlyList<string> subjects, int maxDeliver, CancellationToken cancellationToken)
    {
        INatsJSContext context = connection.Context;

        _ = await context.CreateOrUpdateStreamAsync(
            new StreamConfig(_options.QualifiedStreamName, [.. subjects])
            {
                Retention = await ResolveRetentionAsync(context, cancellationToken),
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
                MaxDeliver = maxDeliver,
                FilterSubjects = [.. subjects]
            },
            cancellationToken);
    }

    private async Task<StreamConfigRetention> ResolveRetentionAsync(INatsJSContext context, CancellationToken cancellationToken)
    {
        StreamConfigRetention existing;

        try
        {
            INatsJSStream stream = await context.GetStreamAsync(_options.QualifiedStreamName, cancellationToken: cancellationToken);
            existing = stream.Info.Config.Retention;
        }
        catch (NatsJSApiException exception) when (exception.Error.Code == StreamNotFound)
        {
            return Retention;
        }

        if (existing != Retention)
        {
            logger.LogWarning(
                "stream_retention_mismatch stream={Stream} retention={Retention} requested={Requested}",
                _options.QualifiedStreamName,
                existing,
                Retention);
        }

        return existing;
    }
}
