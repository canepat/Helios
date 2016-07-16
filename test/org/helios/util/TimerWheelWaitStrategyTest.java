package org.helios.util;

import org.agrona.TimerWheel;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TimerWheelWaitStrategyTest
{
    @Test
    public void shouldSkipIdleWhenWorkCountIsPositive()
    {
        final TimerWheel timerWheel = new TimerWheelStub(100, TimeUnit.MILLISECONDS, 4, false);
        final IdleStrategy idleStrategy = new DelegateIdleStrategy(false);

        final TimerWheelWaitStrategy strategy = new TimerWheelWaitStrategy(timerWheel, idleStrategy);
        strategy.idle(1);
    }

    @Test
    public void shouldDelegateIdleWhenNoTimerExpired()
    {
        final TimerWheel timerWheel = new TimerWheelStub(100, TimeUnit.MILLISECONDS, 4, true);
        final IdleStrategy idleStrategy = new DelegateIdleStrategy(true);

        final TimerWheelWaitStrategy strategy = new TimerWheelWaitStrategy(timerWheel, idleStrategy);
        strategy.idle();
        strategy.idle(0);
    }

    @Test
    public void shouldNotDelegateIdleWhenTimerExpired() throws InterruptedException
    {
        final TimerWheel timerWheel = new TimerWheelStub(100, TimeUnit.MILLISECONDS, 4, true);
        final IdleStrategy idleStrategy = new DelegateIdleStrategy(false);

        final TimerWheelWaitStrategy strategy = new TimerWheelWaitStrategy(timerWheel, idleStrategy);

        timerWheel.newTimeout(1, TimeUnit.MILLISECONDS, ()->{});
        Thread.sleep(5);
        strategy.idle();

        timerWheel.newTimeout(1, TimeUnit.MILLISECONDS, () -> {});
        Thread.sleep(5);
        strategy.idle(0);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenTimerWheelIsNull()
    {
        new TimerWheelWaitStrategy(null, new BusySpinIdleStrategy());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenIdleStrategyIsNull()
    {
        new TimerWheelWaitStrategy(new TimerWheel(100, TimeUnit.MILLISECONDS, 4), null);
    }

    private class TimerWheelStub extends TimerWheel
    {
        private boolean condition;

        TimerWheelStub(long tickDuration, TimeUnit timeUnit, int ticksPerWheel, boolean condition)
        {
            super(tickDuration, timeUnit, ticksPerWheel);
            this.condition = condition;
        }

        @Override
        public int expireTimers()
        {
            assertTrue(condition);
            return super.expireTimers();
        }
    }

    private class DelegateIdleStrategy implements IdleStrategy
    {
        private boolean condition;

        DelegateIdleStrategy(boolean condition)
        {
            this.condition = condition;
        }

        @Override
        public void idle(int workCount)
        {
            assertTrue(condition);
        }

        @Override
        public void idle()
        {
            assertTrue(condition);
        }

        @Override
        public void reset()
        {
        }
    }
}
