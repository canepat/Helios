package org.helios.util;

import org.helios.infra.Processor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertTrue;

public class ProcessorHelperTest
{
    @Test
    public void shouldCallProcessorStart()
    {
        ProcessorHelper.start(new ProcessorStub(() -> assertTrue(true)));
    }

    @Test
    public void shouldIgnoreNullProcessor()
    {
        ProcessorHelper.start(null);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionWhenProcessorThrowsDuringStart()
    {
        ProcessorHelper.start(new ProcessorStub(() -> { throw new RuntimeException(); }));
    }

    private class ProcessorStub implements Processor
    {
        private Runnable runnableCode;

        ProcessorStub(final Runnable runnableCode)
        {
            this.runnableCode = runnableCode;
        }

        @Override
        public void start()
        {
            runnableCode.run();
        }

        @Override
        public void close() throws Exception
        {
        }

        @Override
        public void run()
        {
        }
    }
}
