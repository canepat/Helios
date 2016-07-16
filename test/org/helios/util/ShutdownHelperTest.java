package org.helios.util;

import org.junit.Test;

public class ShutdownHelperTest
{
    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenShutdownHandlerIsNull()
    {
        ShutdownHelper.register(null);
    }
}
