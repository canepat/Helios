package org.helios.util;

import sun.misc.Signal;

import java.util.Objects;

public class SigTerm
{
    public static void register(final Runnable task)
    {
        Objects.requireNonNull(task);

        Signal.handle(new Signal("TERM"), (signal) -> task.run());
    }
}
