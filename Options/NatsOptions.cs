using System.ComponentModel.DataAnnotations;
using System.Text.Json;

namespace Messaging.Options;

public sealed class NatsOptions
{
    private const string Scheme = "nats://";

    public const string DefaultHost = "localhost";

    public const ushort DefaultPort = 4222;

    public static string SectionName => "Nats";

    [Required]
    public string Host { get; set; } = DefaultHost;

    [Range(1, ushort.MaxValue)]
    public ushort Port { get; set; } = DefaultPort;

    public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

    public string Url => string.Concat(Scheme, Host, ":", Port.ToString());
}
