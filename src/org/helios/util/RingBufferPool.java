package org.helios.util;

import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public final class RingBufferPool
{
    private final Map<AeronStream, RingBuffer> ringBufferMap;

    public RingBufferPool()
    {
        ringBufferMap = new HashMap<>();
    }

    public RingBuffer add(final AeronStream aeronStream, final RingBuffer ringBuffer)
    {
        return ringBufferMap.put(aeronStream, ringBuffer);
    }

    public RingBuffer remove(final AeronStream aeronStream)
    {
        return ringBufferMap.remove(aeronStream);
    }

    public RingBuffer get(final AeronStream aeronStream)
    {
        return ringBufferMap.get(aeronStream);
    }

    // FIXME: remove
    public Collection<RingBuffer> ringBuffers()
    {
        return ringBufferMap.values();
    }
}
