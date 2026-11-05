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

        await ProvisionStreamAsync(context, subjects, cancellationToken);

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

    private async Task ProvisionStreamAsync(INatsJSContext context, IReadOnlyList<string> subjects, CancellationToken cancellationToken)
    {
        StreamConfig? existing = await FindStreamConfigAsync(context, cancellationToken);

        if (existing is null)
        {
            _ = await context.CreateOrUpdateStreamAsync(
                new StreamConfig(_options.QualifiedStreamName, [.. subjects])
                {
                    Retention = Retention,
                    DuplicateWindow = _options.DuplicateWindow
                },
                cancellationToken);

            return;
        }

        if (existing.Retention != Retention)
        {
            logger.LogWarning(
                "stream_retention_mismatch stream={Stream} retention={Retention} requested={Requested}",
                _options.QualifiedStreamName,
                existing.Retention,
                Retention);
        }

        existing.Subjects = [.. subjects];
        existing.DuplicateWindow = _options.DuplicateWindow;

        _ = await context.UpdateStreamAsync(existing, cancellationToken);
    }

    private async Task<StreamConfig?> FindStreamConfigAsync(INatsJSContext context, CancellationToken cancellationToken)
    {
        try
        {
            INatsJSStream stream = await context.GetStreamAsync(_options.QualifiedStreamName, cancellationToken: cancellationToken);
            return stream.Info.Config;
        }
        catch (NatsJSApiException exception) when (exception.Error.Code == StreamNotFound)
        {
            return null;
        }
    }
}
