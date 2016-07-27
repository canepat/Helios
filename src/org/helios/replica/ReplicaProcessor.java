package org.helios.replica;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.Processor;

import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicaProcessor implements Processor
{
    private final RingBuffer inputRingBuffer;
    private final ReplicaHandler replicaHandler;
    private final AtomicBoolean running;
    private final Thread replicatorThread;
    private final IdleStrategy idleStrategy;

    public ReplicaProcessor(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy, final ReplicaHandler replicaHandler)
    {
        this.inputRingBuffer = inputRingBuffer;
        this.idleStrategy = idleStrategy;
        this.replicaHandler = replicaHandler;

        running = new AtomicBoolean(false);
        replicatorThread = new Thread(this, "replicator");
    }

    @Override
    public void start()
    {
        running.set(true);
        replicatorThread.start();
    }

    @Override
    public void run()
    {
        while (running.get())
        {
            final int bytesRead = inputRingBuffer.read(replicaHandler);
            idleStrategy.idle(bytesRead);
        }
    }

    @Override
    public void close() throws Exception
    {
        running.set(false);
        replicatorThread.join();
    }
}
