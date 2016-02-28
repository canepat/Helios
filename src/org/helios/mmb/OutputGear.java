/*
 * Copyright 2015 - 2016 Helios Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.helios.mmb;

import com.lmax.disruptor.dsl.Disruptor;
import org.helios.core.engine.OutputBufferEvent;
import org.helios.core.engine.OutputEventHandler;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.agrona.LangUtil;

public class OutputGear implements AutoCloseable, MMBGear<OutputBufferEvent>
{
    private final Disruptor<OutputBufferEvent> outputDisruptor;
    private final MMBPublisher busPublisher;

    public OutputGear(final Disruptor<OutputBufferEvent> outputDisruptor, final Aeron aeron, final String channel, int streamId)
    {
        this.outputDisruptor = outputDisruptor;

        busPublisher = new AeronPublisher(aeron, channel, streamId);

        final OutputEventHandler outputHandler = new OutputEventHandler(busPublisher);
        this.outputDisruptor.handleEventsWith(outputHandler);
    }

    @Override
    public void close() throws Exception
    {
        outputDisruptor.shutdown();
        busPublisher.close();
    }

    @Override
    public Disruptor<OutputBufferEvent> getDisruptor()
    {
        return outputDisruptor;
    }

    @Override
    public void start()
    {
        outputDisruptor.start();
    }

    @Override
    public void stop()
    {
        try
        {
            close();
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
