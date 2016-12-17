package org.helios.status;

import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import java.util.EnumMap;

public class StatusCounters implements AutoCloseable
{
    private final EnumMap<StatusCounterDescriptor, AtomicCounter> counterByDescriptorMap =
        new EnumMap<>(StatusCounterDescriptor.class);

    public StatusCounters(final CountersManager countersManager)
    {
        for (final StatusCounterDescriptor descriptor : StatusCounterDescriptor.values())
        {
            counterByDescriptorMap.put(descriptor, descriptor.newCounter(countersManager));
        }
    }

    public AtomicCounter get(final StatusCounterDescriptor descriptor)
    {
        return counterByDescriptorMap.get(descriptor);
    }

    public void close()
    {
        counterByDescriptorMap.values().forEach(AtomicCounter::close);
    }
}
