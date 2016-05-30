package org.helios.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CheckTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final String MESSAGE = "Error Message";

    @Test
    public void shouldPassWhenExpressionIsTrue()
    {
        Check.enforce(true, MESSAGE);
    }

    @Test
    public void shouldThrowExceptionWhenExpressionIsFalse() throws IllegalArgumentException
    {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(MESSAGE);

        Check.enforce(false, MESSAGE);
    }
}
