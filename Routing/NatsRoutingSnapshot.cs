namespace Messaging.Routing;

internal sealed record NatsRoutingSnapshot(ActionDescriptorCollection Descriptors, EndpointRouter Router);
