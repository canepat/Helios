package org.helios.core.snapshot;

import org.agrona.TimerWheel;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.util.DirectBufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.Assert.assertTrue;

public class SnapshotTimerTest
{
    private final long TICK_DURATION_NS = TimeUnit.MICROSECONDS.toNanos(100);
    private final int TICKS_PER_WHEEL = 512;
    private final int BUFFER_SIZE = (16 * 1024) + TRAILER_LENGTH;

    private final long TIMEOUT_TOLERANCE_NS = TimeUnit.MILLISECONDS.toNanos(20);

    private final TimerWheel timerWheel = new TimerWheel(TICK_DURATION_NS, TimeUnit.MILLISECONDS, TICKS_PER_WHEEL);
    private final RingBuffer ringBuffer = new OneToOneRingBuffer(
        new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(BUFFER_SIZE)));

    private SnapshotTimer snapshotTimer;
    private AtomicBoolean timerWheelRunning;
    private ExecutorService timerExecutor;

    @Before
    public void setUp()
    {
        snapshotTimer = new SnapshotTimer(timerWheel, ringBuffer);
        timerWheelRunning = new AtomicBoolean(true);
        timerExecutor = Executors.newSingleThreadExecutor();
        timerExecutor.execute(() -> {
            while (timerWheelRunning.get()) timerWheel.expireTimers();
        });
    }

    @Test
    public void shouldScheduleNowWithTolerance()
    {
        final long start = System.nanoTime();

        snapshotTimer.start();

        int readBytes;
        do
        {
            readBytes = ringBuffer.read((msgTypeId, buffer, index, length) -> {});
        }
        while (readBytes == 0);

        final long duration = System.nanoTime() - start;
        assertTrue(duration <= TIMEOUT_TOLERANCE_NS);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenTimerWheelIsNull()
    {
        new SnapshotTimer(null, ringBuffer);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenInputRingBufferIsNull()
    {
        new SnapshotTimer(timerWheel, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenSnapshotPeriodIsNegative()
    {
        new SnapshotTimer(timerWheel, ringBuffer, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenSnapshotPeriodIsZero()
    {
        new SnapshotTimer(timerWheel, ringBuffer, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenSnapshotPeriodIsGreaterThanMax()
    {
        new SnapshotTimer(timerWheel, ringBuffer, SnapshotTimer.MAX_SNAPSHOT_PERIOD+1);
    }

    @After
    public void tearDown()
    {
        timerWheelRunning.set(false);
        timerExecutor.shutdown();
    }
}
