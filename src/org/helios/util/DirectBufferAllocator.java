package org.helios.util;

import java.nio.ByteBuffer;

public final class DirectBufferAllocator
{
    public static ByteBuffer allocate(int capacity)
    {
        return ByteBuffer.allocateDirect(capacity);
    }

    public static void free(ByteBuffer buffer)
    {
        ((sun.nio.ch.DirectBuffer)buffer).cleaner().clean();
    }
}
