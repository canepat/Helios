package org.helios.archive;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.helios.infra.Processor;

public class ArchiveProcessor<E> implements Processor
{
    private final Disruptor<E> eventDisruptor;
    private volatile boolean running;
    private final Thread archiveThread;

    @SuppressWarnings("unchecked")
    public ArchiveProcessor(final EventFactory<E> eventFactory, int ringBufferSize, final Class<E> eventClass,
                            final int batchSize, final ArchiveBatchHandler<E> batchHandler)
    {
        eventDisruptor = new Disruptor<>(eventFactory, ringBufferSize, DaemonThreadFactory.INSTANCE);
        eventDisruptor.handleEventsWith(new ArchiveEventHandler<>(eventClass, batchSize, batchHandler));

        running = false;
        archiveThread = new Thread(this, "archiveProcessor");
    }

    @Override
    public String name()
    {
        return archiveThread.getName();
    }

    @Override
    public void start()
    {
        running = true;
        archiveThread.start();
    }

    @Override
    public void run()
    {
        while (running)
        {
            // TODO:
        }
    }

    @Override
    public void close() throws Exception
    {
        running = false;
        archiveThread.join();
    }
}
