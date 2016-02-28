package org.helios.util;

import java.nio.ByteBuffer;

public final class DirectBufferAllocator
{
    public static ByteBuffer allocateDirect(int capacity)
    {
        return ByteBuffer.allocateDirect(capacity);
    }

    public static void freeDirect(ByteBuffer buffer)
    {
        ((sun.nio.ch.DirectBuffer)buffer).cleaner().clean();
    }
}
