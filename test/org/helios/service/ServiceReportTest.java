package org.helios.service;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.*;
import org.helios.infra.InputMessageProcessor;
import org.helios.infra.OutputMessageProcessor;
import org.helios.util.DirectBufferAllocator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.Assert.assertTrue;

public class ServiceReportTest
{
    private final int BUFFER_SIZE = (16 * 1024) + TRAILER_LENGTH;

    private final RingBuffer ringBuffer = new OneToOneRingBuffer(
        new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(BUFFER_SIZE)));

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenInputMessageProcessorIsNull()
    {
        try (final Helios helios = new Helios())
        {
            new ServiceReport(null, new OutputMessageProcessor(ringBuffer, helios.newEmbeddedStream(0),
                new BusySpinIdleStrategy(), ""));
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenOutputMessageProcessorIsNull()
    {
        new ServiceReport(new InputMessageProcessor(ringBuffer, new BusySpinIdleStrategy(), 0, ""), null);
    }
}
