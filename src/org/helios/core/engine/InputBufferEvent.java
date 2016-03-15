package org.helios.core.engine;

import org.helios.util.DirectBufferAllocator;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

public class InputBufferEvent
{
    public static final int DEFAULT_BUFFER_SIZE = Integer.getInteger("helios.core.engine.input_buffer_size", 2 * 1024);

    private MutableDirectBuffer defaultBuffer;
    private MutableDirectBuffer buffer;
    private long eventTime;

    public InputBufferEvent()
    {
        this.buffer = new UnsafeBuffer(DirectBufferAllocator.allocateDirect(DEFAULT_BUFFER_SIZE));
    }

    public MutableDirectBuffer getBuffer()
    {
        return buffer;
    }

    public void growBuffer(int size)
    {
        defaultBuffer = buffer;
        buffer.wrap(DirectBufferAllocator.allocateDirect(size));
    }

    public void resetBuffer()
    {
        DirectBufferAllocator.freeDirect(buffer.byteBuffer());
        buffer = defaultBuffer;
        defaultBuffer = null;
    }

    public long getEventTime()
    {
        return eventTime;
    }

    public void setEventTime(long eventTime)
    {
        this.eventTime = eventTime;
    }

    @Override
    public String toString()
    {
        return "IBE: " + super.toString() + ", eventTime=" + eventTime;
    }
}
