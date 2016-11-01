package org.helios.util;

import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public final class RingBufferPool
{
    private final Map<AeronStream, RingBuffer> outputRingBufferMap;
    private final Map<AeronStream, RingBuffer> eventRingBufferMap;

    public RingBufferPool()
    {
        outputRingBufferMap = new HashMap<>();
        eventRingBufferMap = new HashMap<>();
    }

    public RingBuffer addOutputRingBuffer(final AeronStream aeronStream, final RingBuffer ringBuffer)
    {
        return outputRingBufferMap.put(aeronStream, ringBuffer);
    }

    public RingBuffer addEventRingBuffer(final AeronStream aeronStream, final RingBuffer ringBuffer)
    {
        return eventRingBufferMap.put(aeronStream, ringBuffer);
    }

    public RingBuffer getOutputRingBuffer(final AeronStream aeronStream)
    {
        return outputRingBufferMap.get(aeronStream);
    }

    // FIXME: remove
    public Collection<RingBuffer> outputRingBuffers()
    {
        return outputRingBufferMap.values();
    }

    public Collection<RingBuffer> eventRingBuffers()
    {
        return eventRingBufferMap.values();
    }
}
