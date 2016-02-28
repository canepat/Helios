package org.helios.core.journal.snapshot;

import uk.co.real_logic.agrona.TimerWheel;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

public class SnapshotWriter implements Runnable
{
    private static final long MIDNIGHT_SKEW = Long.getLong("helios.core.snapshot.midnight_skew", 60);

    private final TimerWheel timerWheel;

    public SnapshotWriter(final TimerWheel timerWheel)
    {
        this.timerWheel = timerWheel;
    }

    public void schedule()
    {
        // Schedule for execution on current day midnight.
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime midnight = LocalDateTime.of(now.toLocalDate(), LocalTime.MIDNIGHT);
        Duration durationToMidnight = Duration.between(now, midnight);
        timerWheel.newTimeout(durationToMidnight.getSeconds() + MIDNIGHT_SKEW, TimeUnit.SECONDS, this);
    }

    @Override
    public void run()
    {
        // Take the data snapshot.
        // TODO: take the data snapshot.

        // Schedule the next data snapshot.
        schedule();
    }
}
