using System.ComponentModel.DataAnnotations;

namespace Messaging.Options;

public sealed class ConsumerOptions : IValidatableObject
{
    private const string NameSeparator = "_";

    public const int DefaultMaxDeliver = 5;

    public const int DefaultAckWaitSeconds = 30;

    public const int DefaultPrefetchCount = 10;

    public const int DefaultReconnectDelaySeconds = 10;

    public const int DefaultDuplicateWindowMinutes = 2;

    public static string SectionName => "Consumers";

    public static int[] DefaultBackoffSeconds => [1, 5, 30, 120];

    [Required(AllowEmptyStrings = false)]
    public string StreamName { get; set; } = string.Empty;

    [Required(AllowEmptyStrings = false)]
    public string DurableName { get; set; } = string.Empty;

    public string Prefix { get; set; } = string.Empty;

    [Range(1, int.MaxValue)]
    public int MaxDeliver { get; set; } = DefaultMaxDeliver;

    [Range(1, int.MaxValue)]
    public int AckWaitSeconds { get; set; } = DefaultAckWaitSeconds;

    public int[] BackoffSeconds { get; set; } = [];

    [Range(1, int.MaxValue)]
    public int PrefetchCount { get; set; } = DefaultPrefetchCount;

    [Range(1, int.MaxValue)]
    public int ReconnectDelaySeconds { get; set; } = DefaultReconnectDelaySeconds;

    public int DuplicateWindowMinutes { get; set; } = DefaultDuplicateWindowMinutes;

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (!RetryPolicy.IsValidBackoff(EffectiveBackoffSeconds))
        {
            yield return new ValidationResult(
                $"Every {nameof(BackoffSeconds)} entry must be greater than zero.",
                [nameof(BackoffSeconds)]);
        }
    }

    internal static string DefaultStreamName(string applicationName) => ToName(applicationName).ToUpperInvariant();

    internal static string DefaultDurableName(string applicationName) => ToName(applicationName).ToLowerInvariant();

    internal int[] EffectiveBackoffSeconds => BackoffSeconds.Length == 0 ? DefaultBackoffSeconds : BackoffSeconds;

    internal TimeSpan AckWait => TimeSpan.FromSeconds(AckWaitSeconds);

    internal TimeSpan DuplicateWindow => TimeSpan.FromMinutes(DuplicateWindowMinutes);

    internal TimeSpan ReconnectDelay => TimeSpan.FromSeconds(ReconnectDelaySeconds);

    internal RetryPolicy DefaultRetry => new() { MaxDeliver = MaxDeliver, BackoffSeconds = EffectiveBackoffSeconds };

    internal string QualifiedStreamName => ToName(string.Concat(Prefix, StreamName));

    internal string QualifiedDurableName => ToName(string.Concat(Prefix, DurableName));

    internal string QualifySubject(string subject) => string.Concat(Prefix, subject);

    private static string ToName(string value)
        => value.Replace(NatsSubjects.Separator, NameSeparator, StringComparison.Ordinal);
}
