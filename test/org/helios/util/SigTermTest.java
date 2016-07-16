package org.helios.util;

import org.junit.Test;

public class SigTermTest
{
    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenTaskIsNull()
    {
        SigTerm.register(null);
    }
}
