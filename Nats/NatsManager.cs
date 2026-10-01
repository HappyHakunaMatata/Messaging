using System.Security.Cryptography;
using System.Text.Json;

namespace Messaging.Nats;

public sealed class NatsManager(
    INatsConnectionProvider connection,
    IOptions<NatsOptions> natsOptions,
    IOptions<ConsumerOptions> consumerOptions,
    ILogger<NatsManager> logger) : IPublisher
{
    private readonly JsonSerializerOptions _serializerOptions = natsOptions.Value.SerializerOptions;
    private readonly ConsumerOptions _consumerOptions = consumerOptions.Value;

    public async Task PublishAsync<TPayload>(
        string subject,
        TPayload payload,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        byte[] data = JsonSerializer.SerializeToUtf8Bytes(payload, _serializerOptions);
        string messageId = Convert.ToHexString(SHA256.HashData(data));
        string wireSubject = _consumerOptions.QualifySubject(subject);

        PubAckResponse acknowledgement = await connection.Context.PublishAsync(
            wireSubject,
            data,
            headers: ToHeaders(headers),
            opts: new NatsJSPubOpts { MsgId = messageId },
            cancellationToken: cancellationToken);

        if (acknowledgement.Duplicate)
        {
            logger.LogInformation(
                "message_duplicate_ignored subject={Subject} nats_msg_id={NatsMsgId}",
                wireSubject,
                messageId);

            return;
        }

        logger.LogInformation(
            "message_published subject={Subject} nats_msg_id={NatsMsgId} stream_seq={StreamSeq}",
            wireSubject,
            messageId,
            acknowledgement.Seq);
    }

    private static NatsHeaders? ToHeaders(IReadOnlyDictionary<string, string>? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return null;
        }

        NatsHeaders result = [];

        foreach (KeyValuePair<string, string> header in headers)
        {
            result[header.Key] = header.Value;
        }

        return result;
    }
}
