package org.helios;

import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.helios.core.journal.strategy.JournalStrategy;
import org.helios.core.journal.strategy.PositionalWriteJournalStrategy;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

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

    public static final String JOURNAL_DIR_NAME = getProperty("helios.core.journal.dir", "./");
    public static final int JOURNAL_FILE_SIZE = getInteger("helios.core.journal.file_size", 1024 * 1024 * 1024);
    public static final int JOURNAL_FILE_COUNT = getInteger("helios.core.journal.file_count", 1);
    public static String JOURNAL_STRATEGY = getProperty("helios.core.journal.strategy");
    public static boolean JOURNAL_FLUSHING_ENABLED = getBoolean("helios.core.journal.flushing.enabled");

    public static final String MEDIA_DRIVER_CONF_DIR = getProperty("helios.core.media_driver.conf.dir");
    public static final boolean MEDIA_DRIVER_EMBEDDED = !getBoolean("helios.core.media_driver.external");

    public static final String PUB_IDLE_STRATEGY = getProperty("helios.mmb.aeron_publisher.idle.strategy");
    public static final String SUB_IDLE_STRATEGY = getProperty("helios.mmb.aeron_subscriber.idle.strategy");

    public static final String INPUT_WAIT_STRATEGY = getProperty("helios.core.input_disruptor.wait.strategy");
    public static final String OUTPUT_WAIT_STRATEGY = getProperty("helios.core.output_disruptor.wait.strategy");

    public static final long MAX_SPINS = getLong("helios.core.back_off.idle.strategy.max_spins", 20);
    public static final long MAX_YIELDS = getLong("helios.core.back_off.idle.strategy.max_yields", 50);
    public static final long MIN_PARK_NS = getLong("helios.core.back_off.idle.strategy.min_park_ns", 1);
    public static final long MAX_PARK_NS = getLong("helios.core.back_off.idle.strategy.max_park_ns", 100000);

    public static JournalStrategy journalStrategy()
    {
        return newJournalStrategy(JOURNAL_STRATEGY);
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

    private static JournalStrategy newJournalStrategy(final String strategyClassName)
    {
        final Path journalDir = Paths.get(JOURNAL_DIR_NAME);

        JournalStrategy journalStrategy = null;

        if (strategyClassName == null)
        {
            journalStrategy = new PositionalWriteJournalStrategy(journalDir, JOURNAL_FILE_SIZE, JOURNAL_FILE_COUNT);
        }
        else
        {
            try
            {
                journalStrategy = (JournalStrategy)Class.forName(strategyClassName)
                    .getConstructor(Path.class, Long.class, Integer.class)
                    .newInstance(journalDir, JOURNAL_FILE_SIZE, JOURNAL_FILE_COUNT);
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        return journalStrategy;
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
