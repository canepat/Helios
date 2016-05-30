package org.helios.util;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;

import java.nio.ByteBuffer;

import static org.agrona.BitUtil.isPowerOfTwo;

public final class DirectBufferAllocator
{
    public static ByteBuffer allocate(int capacity)
    {
        return ByteBuffer.allocateDirect(capacity);
    }

    public static ByteBuffer allocateCacheAligned(final int capacity)
    {
        return allocateAligned(capacity, BitUtil.CACHE_LINE_LENGTH);
    }

    public static void free(ByteBuffer buffer)
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

    /** TO BE REMOVED on next Agrona build including BufferUtil.allocateAligned **/
    private static ByteBuffer allocateAligned(final int capacity, final int alignment)
    {
        if (!isPowerOfTwo(alignment))
        {
            throw new IllegalArgumentException("Must be a power of 2: alignment=" + alignment);
        }

        final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity + alignment);

        final long address = BufferUtil.address(buffer);
        final int remainder = (int)(address & (alignment - 1));
        final int offset = alignment - remainder;

        buffer.limit(capacity + offset);
        buffer.position(offset);

        return buffer.slice();
    }
}
