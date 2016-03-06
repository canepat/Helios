package org.helios;

import com.lmax.disruptor.WaitStrategy;
import org.helios.core.journal.strategy.JournalStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

public class HeliosContext
{
    private String mediaDriverConf;
    private boolean mediaDriverEmbedded;
    private boolean replicaEnabled;
    private boolean journalEnabled;
    private boolean journalFlushingEnabled;

    private JournalStrategy journalStrategy;
    private IdleStrategy publisherIdleStrategy;
    private IdleStrategy subscriberIdleStrategy;
    private WaitStrategy inputWaitStrategy;
    private WaitStrategy outputWaitStrategy;

    public HeliosContext()
    {
        setMediaDriverConf(HeliosConfiguration.MEDIA_DRIVER_CONF_DIR);
        setMediaDriverEmbedded(HeliosConfiguration.MEDIA_DRIVER_EMBEDDED);
        setReplicaEnabled(HeliosConfiguration.REPLICA_ENABLED);
        setJournalEnabled(HeliosConfiguration.JOURNAL_ENABLED);
        setJournalFlushingEnabled(HeliosConfiguration.JOURNAL_FLUSHING_ENABLED);
        setJournalStrategy(HeliosConfiguration.journalStrategy());
        setPublisherIdleStrategy(HeliosConfiguration.publisherIdleStrategy());
        setSubscriberIdleStrategy(HeliosConfiguration.subscriberIdleStrategy());
        setInputWaitStrategy(HeliosConfiguration.inputWaitStrategy());
        setOutputWaitStrategy(HeliosConfiguration.outputWaitStrategy());
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

    public HeliosContext setJournalStrategy(JournalStrategy journalStrategy)
    {
        this.journalStrategy = journalStrategy;
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

    public HeliosContext setInputWaitStrategy(WaitStrategy inputWaitStrategy)
    {
        this.inputWaitStrategy = inputWaitStrategy;
        return this;
    }

    public HeliosContext setOutputWaitStrategy(WaitStrategy outputWaitStrategy)
    {
        this.outputWaitStrategy = outputWaitStrategy;
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

    public JournalStrategy getJournalStrategy()
    {
        return journalStrategy;
    }

    public IdleStrategy getPublisherIdleStrategy()
    {
        return publisherIdleStrategy;
    }

    public IdleStrategy getSubscriberIdleStrategy()
    {
        return subscriberIdleStrategy;
    }

    public WaitStrategy getInputWaitStrategy()
    {
        return inputWaitStrategy;
    }

    public WaitStrategy getOutputWaitStrategy()
    {
        return outputWaitStrategy;
    }
}
