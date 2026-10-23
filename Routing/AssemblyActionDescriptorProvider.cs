using System.Runtime.CompilerServices;
using Messaging.Filters;

namespace Messaging.Routing;

internal sealed class AssemblyActionDescriptorProvider(
    HandlerAssemblyCatalog catalog,
    FilterFactory filterFactory,
    IOptions<ConsumerOptions> options,
    IServiceProviderIsService? isService = null) : IActionDescriptorProvider
{
    private readonly RetryPolicy _defaultRetry = options.Value.DefaultRetry;

    public IReadOnlyList<NatsActionDescriptor> GetDescriptors()
    {
        List<NatsActionDescriptor> descriptors = [];

        IReadOnlyList<Assembly> assemblies = catalog.Assemblies;

        for (int assemblyIndex = 0; assemblyIndex < assemblies.Count; assemblyIndex++)
        {
            Type[] handlerTypes = assemblies[assemblyIndex].GetTypes();

            for (int typeIndex = 0; typeIndex < handlerTypes.Length; typeIndex++)
            {
                Type handlerType = handlerTypes[typeIndex];

                if (IsCandidate(handlerType))
                {
                    AddDescriptors(handlerType, descriptors);
                }
            }
        }

        return descriptors;
    }

    private static bool IsCandidate(Type type)
        => type is { IsClass: true, IsAbstract: false, ContainsGenericParameters: false }
           && !type.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false);

    private void AddDescriptors(Type handlerType, List<NatsActionDescriptor> descriptors)
    {
        string? classSubject = handlerType.GetCustomAttribute<SubjectAttribute>()?.Subject;
        RetryPolicy? classRetry = null;

        MethodInfo[] methods = handlerType.GetMethods(BindingFlags.Public | BindingFlags.Instance);

        for (int index = 0; index < methods.Length; index++)
        {
            MethodInfo method = methods[index];

            if (method.IsSpecialName || method.DeclaringType == typeof(object))
            {
                continue;
            }

            string? methodSubject = method.GetCustomAttribute<SubjectAttribute>()?.Subject;
            string? action = method.GetCustomAttribute<ActionAttribute>()?.Action;

            if (methodSubject is null && action is null)
            {
                continue;
            }

            string displayName = $"{handlerType.FullName}.{method.Name}";

            EnsureLiteral(displayName, nameof(SubjectAttribute), classSubject);
            EnsureLiteral(displayName, nameof(SubjectAttribute), methodSubject);
            EnsureLiteral(displayName, nameof(ActionAttribute), action);

            HandlerSignature signature = HandlerSignature.Describe(displayName, method, isService);
            classRetry ??= Overlay(handlerType.FullName!, handlerType.GetCustomAttribute<RetryAttribute>(), _defaultRetry);

            descriptors.Add(new NatsActionDescriptor
            {
                Subject = ResolveSubject(displayName, classSubject, methodSubject, action),
                ClassSubject = methodSubject ?? classSubject,
                Action = action,
                HandlerType = handlerType,
                MethodInfo = method,
                DisplayName = displayName,
                MessageType = signature.MessageType,
                Parameters = signature.Parameters,
                Filters = filterFactory.CreateDescriptors(handlerType, method),
                Retry = Overlay(displayName, method.GetCustomAttribute<RetryAttribute>(), classRetry)
            });
        }
    }

    private static string ResolveSubject(string displayName, string? classSubject, string? methodSubject, string? action)
    {
        if (methodSubject is not null)
        {
            return action is null ? methodSubject : NatsSubjects.Compose(methodSubject, action);
        }

        if (classSubject is null)
        {
            throw new InvalidOperationException(
                $"Handler '{displayName}' declares [{nameof(ActionAttribute)}] but its type declares no [{nameof(SubjectAttribute)}].");
        }

        return NatsSubjects.Compose(classSubject, action!);
    }

    private static RetryPolicy Overlay(string displayName, RetryAttribute? retry, RetryPolicy inherited)
    {
        if (retry is null)
        {
            return inherited;
        }

        if (retry.MaxDeliver < 0)
        {
            throw new InvalidOperationException(
                $"Handler '{displayName}' declares [{nameof(RetryAttribute)}] with a negative {nameof(RetryAttribute.MaxDeliver)}.");
        }

        if (!RetryPolicy.IsValidBackoff(retry.BackoffSeconds))
        {
            throw new InvalidOperationException(
                $"Handler '{displayName}' declares [{nameof(RetryAttribute)}] with a {nameof(RetryAttribute.BackoffSeconds)} entry that is not greater than zero.");
        }

        return new RetryPolicy
        {
            MaxDeliver = retry.MaxDeliver > 0 ? retry.MaxDeliver : inherited.MaxDeliver,
            BackoffSeconds = retry.BackoffSeconds.Length > 0 ? retry.BackoffSeconds : inherited.BackoffSeconds
        };
    }

    private static void EnsureLiteral(string displayName, string attribute, string? value)
    {
        if (value is not null && !NatsSubjects.IsLiteral(value))
        {
            throw new InvalidOperationException(
                $"Handler '{displayName}' declares [{attribute}(\"{value}\")], which is not a literal subject. " +
                "Handlers are matched by exact subject, so wildcards ('*', '>'), empty tokens and whitespace are not supported.");
        }
    }
}
