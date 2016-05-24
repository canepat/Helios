package org.helios.util;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import org.agrona.TimerWheel;

public class TimerWheelWaitStrategy implements WaitStrategy
{
    private final TimerWheel timerWheel;

    public TimerWheelWaitStrategy(final TimerWheel timerWheel)
    {
        this.timerWheel = timerWheel;
    }
    
    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
        throws AlertException
    {
        long availableSequence;
        
        while ((availableSequence = dependentSequence.get()) < sequence)
        {
            barrier.checkAlert();

            int timersExpired = timerWheel.expireTimers();
            if (0 == timersExpired)
            {
                Thread.yield();
            }
        }
        
        return availableSequence;
    }
    
    @Override
    public void signalAllWhenBlocking()
    {
    }
}
