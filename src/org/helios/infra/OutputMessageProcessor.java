package org.helios.infra;

import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;
import org.helios.mmb.HeartbeatMessage;
import org.helios.mmb.StartupMessage;

import static org.agrona.UnsafeAccess.UNSAFE;

public class OutputMessageProcessor implements Processor, OutputReport
{
    private static final long SUCCESSFUL_READS_OFFSET;
    private static final long FAILED_READS_OFFSET;
    private static final long HEARTBEAT_SENT_OFFSET;

    private volatile long successfulBufferReads = 0;
    private volatile long failedBufferReads = 0;
    private volatile long heartbeatSent = 0;

    private final RingBuffer outputRingBuffer;
    private final IdleStrategy idleStrategy;
    private final OutputMessageHandler handler;
    private final int heartbeatInterval;
    private final StartupMessage startupMessage;
    private final HeartbeatMessage heartbeatMessage;
    private volatile boolean running;
    private final Thread processorThread;

    public OutputMessageProcessor(final RingBuffer outputRingBuffer, final AeronStream outputStream,
        final IdleStrategy idleStrategy, final int heartbeatInterval, final String threadName)
    {
        this.outputRingBuffer = outputRingBuffer;
        this.idleStrategy = idleStrategy;
        this.heartbeatInterval = heartbeatInterval;

        handler = new OutputMessageHandler(outputStream, idleStrategy);
        startupMessage = new StartupMessage(outputStream.componentType, outputStream.componentId);
        heartbeatMessage = new HeartbeatMessage(outputStream.componentType, outputStream.componentId);

        running = false;
        processorThread = new Thread(this, threadName);
    }

    @Override
    public String name()
    {
        return processorThread.getName();
    }

    @Override
    public void start()
    {
        running = true;
        processorThread.start();
    }

    @Override
    public void run()
    {
        // Send StartupMessage MMB message
        startupMessage.write(handler);

        long heartbeatTimeMillis = System.currentTimeMillis() + heartbeatInterval;

        while (running)
        {
            final int readCount = outputRingBuffer.read(handler);
            if (0 == readCount)
            {
                UNSAFE.putOrderedLong(this, FAILED_READS_OFFSET, failedBufferReads + 1);

                idleStrategy.idle(0);

                if (System.currentTimeMillis() > heartbeatTimeMillis)
                {
                    heartbeatTimeMillis = System.currentTimeMillis() + heartbeatInterval;
                    heartbeatMessage.write(handler);

                    UNSAFE.putOrderedLong(this, HEARTBEAT_SENT_OFFSET, heartbeatSent + 1);
                }
            }
            else
            {
                UNSAFE.putOrderedLong(this, SUCCESSFUL_READS_OFFSET, successfulBufferReads + 1);
            }
        }
    }

    @Override
    public void close() throws Exception
    {
        running = false;
        processorThread.join();

        CloseHelper.close(handler);
    }

    @Override
    public long successfulWrites()
    {
        return handler.successfulWrites();
    }

    @Override
    public long failedWrites()
    {
        return handler.failedWrites();
    }

    @Override
    public long bytesWritten()
    {
        return handler.bytesWritten();
    }

    @Override
    public long successfulBufferReads()
    {
        return successfulBufferReads;
    }

    @Override
    public long failedBufferReads()
    {
        return failedBufferReads;
    }

    @Override
    public long heartbeatSent()
    {
        return heartbeatSent;
    }

    static
    {
        try
        {
            SUCCESSFUL_READS_OFFSET = UNSAFE.objectFieldOffset(OutputMessageProcessor.class.getDeclaredField("successfulBufferReads"));
            FAILED_READS_OFFSET = UNSAFE.objectFieldOffset(OutputMessageProcessor.class.getDeclaredField("failedBufferReads"));
            HEARTBEAT_SENT_OFFSET = UNSAFE.objectFieldOffset(OutputMessageProcessor.class.getDeclaredField("heartbeatSent"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
