package org.helios.util;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;

import java.nio.ByteBuffer;

public final class DirectBufferAllocator
{
    public static ByteBuffer allocate(int capacity)
    {
        return ByteBuffer.allocateDirect(capacity);
    }

    public static ByteBuffer allocateCacheAligned(final int capacity)
    {
        return BufferUtil.allocateDirectAligned(capacity, BitUtil.CACHE_LINE_LENGTH);
    }

    public static void free(final ByteBuffer buffer)
    {
        sun.nio.ch.DirectBuffer directBuffer = (sun.nio.ch.DirectBuffer)buffer;
        sun.misc.Cleaner cleaner = directBuffer.cleaner();

        while (cleaner == null && directBuffer != null)
        {
            directBuffer = (sun.nio.ch.DirectBuffer)directBuffer.attachment();
            if (directBuffer != null)
            {
                cleaner = directBuffer.cleaner();
            }
        }

        if (cleaner != null)
        {
            cleaner.clean();
        }
    }
}
