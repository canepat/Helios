package org.helios;

import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.helios.core.journal.Journalling;
import org.helios.core.journal.strategy.PositionalJournalling;
import org.agrona.LangUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.Boolean.getBoolean;
import static java.lang.Integer.getInteger;
import static java.lang.Long.getLong;
import static java.lang.System.getProperty;

public class HeliosConfiguration
{
    public static final boolean REPLICA_ENABLED = getBoolean("helios.core.replica.enabled");
    public static final boolean JOURNAL_ENABLED = getBoolean("helios.core.journal.enabled");
    public static final boolean REPORTING_ENABLED = getBoolean("helios.core.reporting.enabled");

    public static final String JOURNAL_DIR_NAME = getProperty("helios.core.journal.dir", "./");
    public static final int JOURNAL_FILE_SIZE = getInteger("helios.core.journal.file_size", 1024 * 1024 * 1024);
    public static final int JOURNAL_FILE_COUNT = getInteger("helios.core.journal.file_count", 1);
    public static final String JOURNAL_STRATEGY = getProperty("helios.core.journal.strategy");
    public static final boolean JOURNAL_FLUSHING_ENABLED = getBoolean("helios.core.journal.flushing.enabled");
    public static final int JOURNAL_PAGE_SIZE = getInteger("helios.core.journal.page_size", 4 * 1024);

    public static final String REPLICA_CHANNEL = getProperty("helios.core.replica_channel", "udp://localhost:40125");
    public static final int REPLICA_STREAM_ID = getInteger("helios.core.replica_stream_id", 10);

    public static final String MEDIA_DRIVER_CONF_DIR = getProperty("helios.core.media_driver.conf.dir");
    public static final boolean MEDIA_DRIVER_EMBEDDED = !getBoolean("helios.core.media_driver.external");

    public static final String READ_IDLE_STRATEGY = getProperty("helios.core.ring_buffer_read.idle.strategy");
    public static final String WRITE_IDLE_STRATEGY = getProperty("helios.core.ring_buffer_write.idle.strategy");

    public static final String PUB_IDLE_STRATEGY = getProperty("helios.mmb.aeron_publisher.idle.strategy");
    public static final String SUB_IDLE_STRATEGY = getProperty("helios.mmb.aeron_subscriber.idle.strategy");

    public static final String INPUT_WAIT_STRATEGY = getProperty("helios.core.input_disruptor.wait.strategy");
    public static final String OUTPUT_WAIT_STRATEGY = getProperty("helios.core.output_disruptor.wait.strategy");

    public static final long MAX_SPINS = getLong("helios.core.back_off.idle.strategy.max_spins", 100);
    public static final long MAX_YIELDS = getLong("helios.core.back_off.idle.strategy.max_yields", 10);
    public static final long MIN_PARK_NS = getLong("helios.core.back_off.idle.strategy.min_park_ns", 1000);
    public static final long MAX_PARK_NS = getLong("helios.core.back_off.idle.strategy.max_park_ns", 100000);

    public static Journalling journalStrategy()
    {
        return newJournalStrategy(JOURNAL_STRATEGY);
    }

    public static IdleStrategy readIdleStrategy()
    {
        return newIdleStrategy(READ_IDLE_STRATEGY);
    }

    public static IdleStrategy writeIdleStrategy()
    {
        return newIdleStrategy(WRITE_IDLE_STRATEGY);
    }

    public static IdleStrategy publisherIdleStrategy()
    {
        return newIdleStrategy(PUB_IDLE_STRATEGY);
    }

    public static IdleStrategy subscriberIdleStrategy()
    {
        return newIdleStrategy(SUB_IDLE_STRATEGY);
    }

    public static WaitStrategy inputWaitStrategy()
    {
        return newWaitStrategy(INPUT_WAIT_STRATEGY);
    }

    public static WaitStrategy outputWaitStrategy()
    {
        return newWaitStrategy(OUTPUT_WAIT_STRATEGY);
    }

    private static Journalling newJournalStrategy(final String journallingClassName)
    {
        final Path journalDir = Paths.get(JOURNAL_DIR_NAME);

        Journalling journalling = null;

        if (journallingClassName == null)
        {
            journalling = new PositionalJournalling(journalDir, JOURNAL_FILE_SIZE, JOURNAL_PAGE_SIZE, JOURNAL_FILE_COUNT);
        }
        else
        {
            try
            {
                journalling = (Journalling)Class.forName(journallingClassName)
                    .getConstructor(Path.class, Long.class, Integer.class)
                        .newInstance(journalDir, JOURNAL_FILE_SIZE, JOURNAL_FILE_COUNT);
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        return journalling;
    }

    private static IdleStrategy newIdleStrategy(final String strategyClassName)
    {
        IdleStrategy idleStrategy = null;

        if (strategyClassName == null)
        {
            idleStrategy = new BackoffIdleStrategy(MAX_SPINS, MAX_YIELDS, MIN_PARK_NS, MAX_PARK_NS);
        }
        else
        {
            try
            {
                idleStrategy = (IdleStrategy)Class.forName(strategyClassName).newInstance();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        return idleStrategy;
    }

    private static WaitStrategy newWaitStrategy(final String strategyClassName)
    {
        WaitStrategy waitStrategy = null;

        if (strategyClassName == null)
        {
            waitStrategy = new YieldingWaitStrategy();
        }
        else
        {
            try
            {
                waitStrategy = (WaitStrategy)Class.forName(strategyClassName).newInstance();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        return waitStrategy;
    }
}
