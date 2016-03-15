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
import org.helios.core.engine.InputBufferEvent;
import org.helios.core.engine.InputFragmentHandler;
import uk.co.real_logic.aeron.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class InputGear implements MMBGear<InputBufferEvent>
{
    private final AtomicBoolean running;
    private final Disruptor<InputBufferEvent> inputDisruptor;
    private final AeronSubscriber busSubscriber;
    private final FragmentAssembler fragmentAssembler;
    private final int fragmentLimit;
    private final ExecutorService executorService;

    public InputGear(final Disruptor<InputBufferEvent> inputDisruptor, final Aeron aeron, final String channel, int streamId)
    {
        this.inputDisruptor = inputDisruptor;

        running = new AtomicBoolean(true);
        busSubscriber = new AeronSubscriber(running, aeron, channel, streamId);
        fragmentAssembler = new FragmentAssembler(new InputFragmentHandler(inputDisruptor));

        fragmentLimit = Integer.getInteger("helios.mmb.fragment_limit", 10);

        executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    public Disruptor<InputBufferEvent> getDisruptor()
    {
        return inputDisruptor;
    }

    @Override
    public void start()
    {
        running.set(true);

        inputDisruptor.start();
        executorService.submit(() -> busSubscriber.receive(fragmentAssembler, fragmentLimit));
    }

    @Override
    public void stop()
    {
        running.set(false);

        executorService.shutdown();
        inputDisruptor.shutdown();
    }
}
