using System.Runtime.CompilerServices;
using Messaging.Filters;

namespace Messaging.Routing;

internal sealed class AssemblyActionDescriptorProvider(
    HandlerAssemblyCatalog catalog,
    FilterFactory filterFactory,
    IServiceProviderIsService? isService = null) : IActionDescriptorProvider
{
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
            HandlerSignature signature = HandlerSignature.Describe(displayName, method, isService);

            descriptors.Add(new NatsActionDescriptor
            {
                Subject = ResolveSubject(displayName, classSubject, methodSubject, action),
                ClassSubject = classSubject,
                Action = action,
                HandlerType = handlerType,
                MethodInfo = method,
                DisplayName = displayName,
                MessageType = signature.MessageType,
                Parameters = signature.Parameters,
                Filters = filterFactory.CreateDescriptors(handlerType, method)
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
}
