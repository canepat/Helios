package org.helios.util;

import org.agrona.TimerWheel;
import org.agrona.concurrent.IdleStrategy;

import java.util.Objects;

public class TimerWheelWaitStrategy implements IdleStrategy
{
    private final TimerWheel timerWheel;
    private final IdleStrategy delegate;

    public TimerWheelWaitStrategy(final TimerWheel timerWheel, final IdleStrategy delegate)
    {
        this.timerWheel = Objects.requireNonNull(timerWheel);
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public void idle(int workCount)
    {
        if (workCount > 0)
        {
            return;
        }

        int timersExpired = timerWheel.expireTimers();
        if (timersExpired > 0)
        {
            return;
        }

        delegate.idle(workCount);
    }

    @Override
    public void idle()
    {
        int timersExpired = timerWheel.expireTimers();
        if (timersExpired > 0)
        {
            return;
        }

        delegate.idle();
    }

    @Override
    public void reset()
    {
    }
}
