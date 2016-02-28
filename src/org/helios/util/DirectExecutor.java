package org.helios.util;

import java.util.concurrent.Executor;

public class DirectExecutor implements Executor
{
    @Override
    public void execute(Runnable runnable)
    {
        // Execute provided runnable task directly using calling thread.
        runnable.run();
    }
}
