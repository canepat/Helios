package org.helios.snapshot;

import org.agrona.TimerWheel;
import org.agrona.Verify;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.util.Check;

import java.util.concurrent.TimeUnit;

public final class SnapshotTimer implements Runnable, AutoCloseable
{
    public static final long MAX_SNAPSHOT_PERIOD = TimeUnit.HOURS.toMillis(24);
    public static final long DEFAULT_SNAPSHOT_PERIOD = TimeUnit.HOURS.toMillis(24);

    private final TimerWheel timerWheel;
    private final RingBuffer inputRingBuffer;
    private final long snapshotPeriod;
    private TimerWheel.Timer timer;

    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

    public SnapshotTimer(final TimerWheel timerWheel, final RingBuffer inputRingBuffer)
    {
        this(timerWheel, inputRingBuffer, DEFAULT_SNAPSHOT_PERIOD);
    }

    public SnapshotTimer(final TimerWheel timerWheel, final RingBuffer inputRingBuffer, final long snapshotPeriod)
    {
        Verify.notNull(timerWheel, "timerWheel");
        Verify.notNull(inputRingBuffer, "inputRingBuffer");
        Verify.notNull(snapshotPeriod, "snapshotPeriod");
        Check.enforce(0 < snapshotPeriod && snapshotPeriod <= MAX_SNAPSHOT_PERIOD, "snapshotPeriod out of range");

        this.timerWheel = timerWheel;
        this.inputRingBuffer = inputRingBuffer;
        this.snapshotPeriod = snapshotPeriod;
    }

    public void start()
    {
        // Schedule for immediate execution.
        timer = timerWheel.newTimeout(0, TimeUnit.MILLISECONDS, this);
    }

    @Override
    public void run()
    {
        // Write the Save Data Snapshot message into the input pipeline.
        Snapshot.writeSaveMessage(inputRingBuffer, idleStrategy);

        // Schedule the next data snapshot after periodic time interval.
        timerWheel.rescheduleTimeout(snapshotPeriod, TimeUnit.MILLISECONDS, timer);
    }

    @Override
    public void close() throws Exception
    {
        timer.cancel();
    }
}
