package org.helios.mmb;

import com.lmax.disruptor.dsl.Disruptor;

public interface MMBGear<T>
{
    Disruptor<T> getDisruptor();

    void start();

    void stop();
}
