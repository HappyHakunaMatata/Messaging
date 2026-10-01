using System.ComponentModel.DataAnnotations;

namespace Messaging.Options;

public sealed class ConsumerOptions : IValidatableObject
{
    private const string NameSeparator = "_";

    public const string DefaultStreamName = "PROJECT_CONSUMERS";

    public const string DefaultDurableName = "project-consumers";

    public const int DefaultMaxDeliver = 5;

    public const int DefaultReconnectDelaySeconds = 10;

    public const int DefaultDuplicateWindowMinutes = 2;

    public static string SectionName => "Consumers";

    public static int[] DefaultBackoffSeconds => [1, 5, 30, 120];

    [Required(AllowEmptyStrings = false)]
    public string StreamName { get; set; } = DefaultStreamName;

    [Required(AllowEmptyStrings = false)]
    public string DurableName { get; set; } = DefaultDurableName;

    public string Prefix { get; set; } = string.Empty;

    [Range(1, int.MaxValue)]
    public int MaxDeliver { get; set; } = DefaultMaxDeliver;

    public int[] BackoffSeconds { get; set; } = [];

    [Range(1, int.MaxValue)]
    public int ReconnectDelaySeconds { get; set; } = DefaultReconnectDelaySeconds;

    public int DuplicateWindowMinutes { get; set; } = DefaultDuplicateWindowMinutes;

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (MaxDeliver <= EffectiveBackoffSeconds.Length)
        {
            yield return new ValidationResult(
                $"{nameof(MaxDeliver)} ({MaxDeliver}) must be greater than the number of {nameof(BackoffSeconds)} entries ({EffectiveBackoffSeconds.Length}). " +
                "JetStream refuses to create a consumer whose backoff list is not shorter than its delivery limit.",
                [nameof(MaxDeliver), nameof(BackoffSeconds)]);
        }

        if (EffectiveBackoffSeconds.Any(seconds => seconds <= 0))
        {
            yield return new ValidationResult(
                $"Every {nameof(BackoffSeconds)} entry must be greater than zero.",
                [nameof(BackoffSeconds)]);
        }
    }

    internal int[] EffectiveBackoffSeconds => BackoffSeconds.Length == 0 ? DefaultBackoffSeconds : BackoffSeconds;

    internal TimeSpan[] Backoff => [.. EffectiveBackoffSeconds.Select(seconds => TimeSpan.FromSeconds(seconds))];

    internal TimeSpan DuplicateWindow => TimeSpan.FromMinutes(DuplicateWindowMinutes);

    internal TimeSpan ReconnectDelay => TimeSpan.FromSeconds(ReconnectDelaySeconds);

    internal string QualifiedStreamName => ToName(string.Concat(Prefix, StreamName));

    internal string QualifiedDurableName => ToName(string.Concat(Prefix, DurableName));

    internal string QualifySubject(string subject) => string.Concat(Prefix, subject);

    private static string ToName(string value)
        => value.Replace(NatsSubjects.Separator, NameSeparator, StringComparison.Ordinal);
}
