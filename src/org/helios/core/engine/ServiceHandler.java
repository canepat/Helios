package org.helios.core.engine;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public interface ServiceHandler extends EventHandler<InputBufferEvent>, LifecycleAware, AutoCloseable
{
}
