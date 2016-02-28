package org.helios.util;

import sun.misc.Signal;

public class SigTerm
{
    public static void register(Runnable task)
    {
        Signal.handle(new Signal("TERM"), (signal) -> task.run());
    }
}
