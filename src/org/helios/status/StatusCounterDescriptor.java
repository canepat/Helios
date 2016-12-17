package org.helios.status;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

public enum StatusCounterDescriptor
{
    BYTES_SENT(0, "Bytes sent"),
    BYTES_RECEIVED(1, "Bytes received"),
    HEARTBEATS_SENT(2, "Heartbeats sent"),
    HEARTBEATS_RECEIVED(3, "Heartbeats received"),
    ERRORS(4, "Errors");

    public static final int SYSTEM_COUNTER_TYPE_ID = 0;

    private static final Int2ObjectHashMap<StatusCounterDescriptor> DESCRIPTOR_BY_ID_MAP = new Int2ObjectHashMap<>();

    static
    {
        for (final StatusCounterDescriptor descriptor : StatusCounterDescriptor.values())
        {
            if (null != DESCRIPTOR_BY_ID_MAP.put(descriptor.id, descriptor))
            {
                throw new IllegalStateException("Descriptor id already in use: " + descriptor.id);
            }
        }
    }

    public static StatusCounterDescriptor get(final int id)
    {
        return DESCRIPTOR_BY_ID_MAP.get(id);
    }

    private final int id;
    private final String label;

    StatusCounterDescriptor(final int id, final String label)
    {
        this.id = id;
        this.label = label;
    }

    public int id()
    {
        return id;
    }

    public String label()
    {
        return label;
    }

    public AtomicCounter newCounter(final CountersManager countersManager)
    {
        return countersManager.newCounter(label, SYSTEM_COUNTER_TYPE_ID, (buffer) -> buffer.putInt(0, id));
    }
}
