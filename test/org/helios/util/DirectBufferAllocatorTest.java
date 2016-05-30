package org.helios.util;

import org.agrona.BitUtil;
import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DirectBufferAllocatorTest
{
    private static final int CAPACITY = 100;

    @Test
    public void shouldAllocateDirectBufferWithSpecifiedCapacity()
    {
        final ByteBuffer buffer = DirectBufferAllocator.allocate(CAPACITY);

        assertTrue(buffer.isDirect());
        assertThat(buffer.capacity(), is(CAPACITY));
    }

    @Test
    public void shouldAllocateCacheAlignedDirectBufferWithSpecifiedCapacity()
    {
        final ByteBuffer buffer = DirectBufferAllocator.allocateCacheAligned(CAPACITY);

        assertTrue(buffer.isDirect());
        assertThat(buffer.capacity(), is(CAPACITY));

        final long address = ((DirectBuffer)buffer).address();
        assertTrue(BitUtil.isAligned(address, BitUtil.CACHE_LINE_LENGTH));
    }

    @Test
    public void shouldFreeDirectBuffer()
    {
        final ByteBuffer buffer = DirectBufferAllocator.allocate(CAPACITY);
        DirectBufferAllocator.free(buffer);
    }

    @Test
    public void shouldFreeCacheAlignedDirectBuffer()
    {
        final ByteBuffer buffer = DirectBufferAllocator.allocateCacheAligned(CAPACITY);
        DirectBufferAllocator.free(buffer);
    }
}
