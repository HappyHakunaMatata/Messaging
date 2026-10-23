namespace Messaging.Routing;

internal interface IActionDescriptorProvider
{
    IReadOnlyList<NatsActionDescriptor> GetDescriptors();
}
