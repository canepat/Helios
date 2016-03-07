package org.helios.gateway;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import org.helios.core.engine.InputBufferEvent;

public interface ServiceProxy extends EventHandler<InputBufferEvent>, LifecycleAware,  AutoCloseable
{
}
