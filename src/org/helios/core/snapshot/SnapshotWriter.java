package org.helios.core.snapshot;

import org.agrona.TimerWheel;
import org.agrona.Verify;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

public class SnapshotWriter implements Runnable
{
    private static final long TIMEOUT_SKEW = Long.getLong("helios.core.snapshot.timeout_skew", 60);

    private final TimerWheel timerWheel;
    private final Snapshot snapshot;

    public SnapshotWriter(final TimerWheel timerWheel, final Snapshot snapshot)
    {
        Verify.notNull(timerWheel, "timerWheel");
        Verify.notNull(snapshot, "snapshot");

        this.timerWheel = timerWheel;
        this.snapshot = snapshot;
    }

    public void scheduleNow()
    {
        // Schedule for immediate execution.
        timerWheel.newTimeout(TIMEOUT_SKEW, TimeUnit.MILLISECONDS, this);
    }

    @Override
    public void run()
    {
        // Take the data snapshot.
        try
        {
            snapshot.save();
        }
        catch (IOException e)
        {
        }

        // Schedule the next data snapshot at current day midnight.
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime midnight = LocalDateTime.of(now.toLocalDate(), LocalTime.MIDNIGHT);
        Duration durationToTarget = Duration.between(now, midnight);
        timerWheel.newTimeout(durationToTarget.getSeconds() + TIMEOUT_SKEW, TimeUnit.SECONDS, this);
    }
}
