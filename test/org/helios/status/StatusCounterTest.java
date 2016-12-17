package org.helios.status;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StatusCounterTest
{
    private static final int VALUES_BUFFER_SIZE = 1024;
    private static final AtomicBuffer METADATA_BUFFER = new UnsafeBuffer(ByteBuffer.allocate(VALUES_BUFFER_SIZE * 2));
    private static final AtomicBuffer VALUES_BUFFER = new UnsafeBuffer(ByteBuffer.allocate(VALUES_BUFFER_SIZE));
    private static final CountersManager COUNTERS_MANAGER = new CountersManager(METADATA_BUFFER, VALUES_BUFFER);

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenCountersManagerIsNull()
    {
        new StatusCounters(null);
    }

    @Test
    public void shouldReturnNullCounterWhenStatusCounterDescriptorIsNull()
    {
        try(final StatusCounters statusCounters = new StatusCounters(COUNTERS_MANAGER))
        {
            final AtomicCounter counter = statusCounters.get(null);
            assertNull(counter);
        }
    }

    @Test
    public void shouldReturnCorrectStatusCounterDescriptor()
    {
        try(final StatusCounters statusCounters = new StatusCounters(COUNTERS_MANAGER))
        {
            for (final StatusCounterDescriptor descriptor : StatusCounterDescriptor.values())
            {
                final AtomicCounter counter = statusCounters.get(descriptor);
                assertNotNull(counter);

                counter.add(descriptor.id());
            }

            for (final StatusCounterDescriptor descriptor : StatusCounterDescriptor.values())
            {
                final AtomicCounter counter = statusCounters.get(descriptor);
                assertNotNull(counter);

                assertTrue(counter.get() == descriptor.id());
            }
        }
    }
}
