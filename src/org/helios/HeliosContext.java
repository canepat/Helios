package org.helios;

import org.agrona.concurrent.IdleStrategy;
import org.helios.journal.Journalling;

public class HeliosContext
{
    private String mediaDriverConf;
    private boolean mediaDriverEmbedded;
    private boolean reportingEnabled;
    private boolean replicaEnabled;
    private boolean journalEnabled;
    private boolean journalFlushingEnabled;

    private String replicaChannel;
    private int replicaStreamId;

    private Journalling journalling;
    private IdleStrategy readIdleStrategy;
    private IdleStrategy writeIdleStrategy;
    private IdleStrategy publisherIdleStrategy;
    private IdleStrategy subscriberIdleStrategy;

    private int heartbeatInterval;
    private int heartbeatLiveness;

    public HeliosContext()
    {
        setMediaDriverConf(HeliosConfiguration.MEDIA_DRIVER_CONF);
        setMediaDriverEmbedded(HeliosConfiguration.MEDIA_DRIVER_EMBEDDED);
        setReportingEnabled(HeliosConfiguration.REPORTING_ENABLED);
        setReplicaEnabled(HeliosConfiguration.REPLICA_ENABLED);
        setJournalEnabled(HeliosConfiguration.JOURNAL_ENABLED);
        setJournalFlushingEnabled(HeliosConfiguration.JOURNAL_FLUSHING_ENABLED);

        setReplicaChannel(HeliosConfiguration.REPLICA_CHANNEL);
        setReplicaStreamId(HeliosConfiguration.REPLICA_STREAM_ID);

        setJournalStrategy(HeliosConfiguration.journalStrategy());
        setReadIdleStrategy(HeliosConfiguration.readIdleStrategy());
        setWriteIdleStrategy(HeliosConfiguration.writeIdleStrategy());
        setPublisherIdleStrategy(HeliosConfiguration.publisherIdleStrategy());
        setSubscriberIdleStrategy(HeliosConfiguration.subscriberIdleStrategy());

        setHeartbeatInterval(HeliosConfiguration.HEARTBEAT_INTERVAL_MS);
        setHeartbeatLiveness(HeliosConfiguration.HEARTBEAT_LIVENESS);
    }

    public HeliosContext setMediaDriverConf(String mediaDriverConf)
    {
        this.mediaDriverConf = mediaDriverConf;
        return this;
    }

    public HeliosContext setMediaDriverEmbedded(boolean mediaDriverEmbedded)
    {
        this.mediaDriverEmbedded = mediaDriverEmbedded;
        return this;
    }

    public HeliosContext setReportingEnabled(boolean reportingEnabled)
    {
        this.reportingEnabled = reportingEnabled;
        return this;
    }

    public HeliosContext setReplicaEnabled(boolean replicaEnabled)
    {
        this.replicaEnabled = replicaEnabled;
        return this;
    }

    public HeliosContext setJournalEnabled(boolean journalEnabled)
    {
        this.journalEnabled = journalEnabled;
        return this;
    }

    public HeliosContext setJournalFlushingEnabled(boolean journalFlushingEnabled)
    {
        this.journalFlushingEnabled = journalFlushingEnabled;
        return this;
    }

    public HeliosContext setReplicaChannel(String replicaChannel)
    {
        this.replicaChannel = replicaChannel;
        return this;
    }

    public HeliosContext setReplicaStreamId(int replicaStreamId)
    {
        this.replicaStreamId = replicaStreamId;
        return this;
    }

    public HeliosContext setJournalStrategy(Journalling journalling)
    {
        this.journalling = journalling;
        return this;
    }

    public HeliosContext setReadIdleStrategy(IdleStrategy readIdleStrategy)
    {
        this.readIdleStrategy = readIdleStrategy;
        return this;
    }

    public HeliosContext setWriteIdleStrategy(IdleStrategy writeIdleStrategy)
    {
        this.writeIdleStrategy = writeIdleStrategy;
        return this;
    }

    public HeliosContext setPublisherIdleStrategy(IdleStrategy publisherIdleStrategy)
    {
        this.publisherIdleStrategy = publisherIdleStrategy;
        return this;
    }

    public HeliosContext setSubscriberIdleStrategy(IdleStrategy subscriberIdleStrategy)
    {
        this.subscriberIdleStrategy = subscriberIdleStrategy;
        return this;
    }

    public HeliosContext setHeartbeatInterval(int heartbeatInterval)
    {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public HeliosContext setHeartbeatLiveness(int heartbeatLiveness)
    {
        this.heartbeatLiveness = heartbeatLiveness;
        return this;
    }

    public String getMediaDriverConf()
    {
        return mediaDriverConf;
    }

    public boolean isMediaDriverEmbedded()
    {
        return mediaDriverEmbedded;
    }

    public boolean isReportingEnabled()
    {
        return reportingEnabled;
    }

    public boolean isReplicaEnabled()
    {
        return replicaEnabled;
    }

    public boolean isJournalEnabled()
    {
        return journalEnabled;
    }

    public boolean isJournalFlushingEnabled()
    {
        return journalFlushingEnabled;
    }

    public String replicaChannel()
    {
        return replicaChannel;
    }

    public int replicaStreamId()
    {
        return replicaStreamId;
    }

    public Journalling journalStrategy()
    {
        return journalling;
    }

    public IdleStrategy readIdleStrategy()
    {
        return readIdleStrategy;
    }

    public IdleStrategy writeIdleStrategy()
    {
        return writeIdleStrategy;
    }

    public IdleStrategy publisherIdleStrategy()
    {
        return publisherIdleStrategy;
    }

    public IdleStrategy subscriberIdleStrategy()
    {
        return subscriberIdleStrategy;
    }

    public int heartbeatInterval()
    {
        return heartbeatInterval;
    }

    public int heartbeatLiveness()
    {
        return heartbeatLiveness;
    }
}
