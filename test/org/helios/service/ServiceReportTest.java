package org.helios.service;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.Helios;
import org.helios.HeliosContext;
import org.helios.HeliosDriver;
import org.helios.infra.InputMessageProcessor;
import org.helios.infra.OutputMessageProcessor;
import org.helios.util.DirectBufferAllocator;
import org.junit.Test;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class ServiceReportTest
{
    private final int BUFFER_SIZE = (16 * 1024) + TRAILER_LENGTH;

    private final RingBuffer ringBuffer = new OneToOneRingBuffer(
        new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(BUFFER_SIZE)));

    private final HeliosContext context = new HeliosContext();
    private final HeliosDriver driver = new HeliosDriver(context);
    private final Helios helios = new Helios(context, driver);

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenInputMessageProcessorIsNull()
    {
        new ServiceReport(
            new InputMessageProcessor(ringBuffer, helios.newStream(null, 0), new BusySpinIdleStrategy(), 0, ""),
            null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenOutputMessageProcessorIsNull()
    {
        new ServiceReport(
            null,
            new OutputMessageProcessor(ringBuffer, helios.newStream(null, 0), new BusySpinIdleStrategy(), ""));
    }
}
