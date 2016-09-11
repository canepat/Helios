package org.helios.util;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.ringbuffer.RingBuffer;

public final class RingBufferPool
{
    private final Long2ObjectHashMap<RingBuffer> ringBufferMap;

    public RingBufferPool()
    {
        ringBufferMap = new Long2ObjectHashMap<>();
    }

    public RingBuffer add(final long ringBufferId, final RingBuffer ringBuffer)
    {
        return ringBufferMap.put(ringBufferId, ringBuffer);
    }

    public RingBuffer remove(final long ringBufferId)
    {
        return ringBufferMap.remove(ringBufferId);
    }

    public RingBuffer get(final long ringBufferId)
    {
        return ringBufferMap.get(ringBufferId);
    }
}
