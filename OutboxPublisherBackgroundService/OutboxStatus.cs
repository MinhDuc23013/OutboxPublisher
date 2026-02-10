namespace OutboxPublisherBackgroundService
{
    public enum OutboxStatus
    {
        New,
        Processing,
        Published,
        Failed
    }
}
