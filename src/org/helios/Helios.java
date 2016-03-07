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
package org.helios;

import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.helios.core.engine.InputBufferEvent;
import org.helios.core.engine.OutputBufferEvent;
import org.helios.core.engine.ServiceHandler;
import org.helios.core.engine.ServiceHandlerFactory;
import org.helios.core.journal.Journaller;
import org.helios.core.journal.strategy.JournalStrategy;
import org.helios.core.replica.Replicator;
import org.helios.gateway.ServiceProxy;
import org.helios.gateway.ServiceProxyFactory;
import org.helios.mmb.InputGear;
import org.helios.mmb.OutputGear;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public class Helios implements AutoCloseable, ErrorHandler
{
    private final HeliosContext context;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private Journaller inputJournaller;
    private Replicator replicator;
    private Consumer<Throwable> errorHandler;

    public Helios()
    {
        this(new HeliosContext());
    }

    public Helios(final HeliosContext context)
    {
        this(context, new MediaDriver.Context()
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
                .receiverIdleStrategy(new NoOpIdleStrategy())
                .senderIdleStrategy(new NoOpIdleStrategy()));
    }

    public Helios(final HeliosContext context, final MediaDriver.Context driverContext)
    {
        this.context = context;

        String mediaDriverConf = context.getMediaDriverConf();
        if (mediaDriverConf != null)
        {
            MediaDriver.loadPropertiesFile(mediaDriverConf);
        }

        final boolean embeddedMediaDriver = context.isMediaDriverEmbedded();
        mediaDriver = embeddedMediaDriver ? MediaDriver.launchEmbedded(driverContext) : null;

        Aeron.Context aeronContext = new Aeron.Context().errorHandler(this);
        if (embeddedMediaDriver)
        {
            aeronContext.aeronDirectoryName(mediaDriver.aeronDirectoryName());
        }
        aeron = Aeron.connect(aeronContext);

        final boolean journalEnabled = context.isJournalEnabled();
        if (journalEnabled)
        {
            final JournalStrategy journalStrategy = context.getJournalStrategy();
            final boolean flushingEnabled = context.isJournalFlushingEnabled();
            inputJournaller = new Journaller(journalStrategy, flushingEnabled);
        }

        final boolean replicaEnabled = context.isReplicaEnabled();
        if (replicaEnabled)
        {
            replicator = new Replicator(aeron);
        }

        errorHandler = System.err::println;
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }

    @Override
    public void onError(Throwable throwable)
    {
        errorHandler.accept(throwable);
    }

    public void setErrorHandler(Consumer<Throwable> errorHandler)
    {
        this.errorHandler = errorHandler;
    }

    public InputGear addInputGear(int bufferSize, final String channel, int streamId)
    {
        final ThreadFactory threadFactory = Executors.privilegedThreadFactory();
        final WaitStrategy strategy = context.getInputWaitStrategy();
        final Disruptor<InputBufferEvent> disruptor = new Disruptor<>(InputBufferEvent::new, bufferSize, threadFactory, ProducerType.SINGLE, strategy);

        return new InputGear(disruptor, aeron, channel, streamId);
    }

    public OutputGear addOutputGear(int bufferSize, final String channel, int streamId)
    {
        final ThreadFactory threadFactory = Executors.privilegedThreadFactory();
        final WaitStrategy strategy = context.getOutputWaitStrategy();
        final Disruptor<OutputBufferEvent> disruptor = new Disruptor<>(OutputBufferEvent::new, bufferSize, threadFactory, ProducerType.SINGLE, strategy);

        return new OutputGear(disruptor, aeron, channel, streamId);
    }

    public Helios addServiceHandler(final ServiceHandlerFactory factory, final InputGear inputGear, final OutputGear outputGear)
    {
        final ServiceHandler serviceHandler = factory.createServiceHandler(this, outputGear);

        if (inputJournaller != null)
        {
            if (replicator != null)
            {
                inputGear.getDisruptor().handleEventsWith(inputJournaller, replicator).handleEventsWith(serviceHandler);
            }
            else
            {
                inputGear.getDisruptor().handleEventsWith(inputJournaller).then(serviceHandler);
            }
        }
        else
        {
            if (replicator != null)
            {
                inputGear.getDisruptor().handleEventsWith(replicator).then(serviceHandler);
            }
            else
            {
                inputGear.getDisruptor().handleEventsWith(serviceHandler);
            }
        }

        return this;
    }

    public Helios addServiceProxy(final ServiceProxyFactory factory, final InputGear inputGear, final OutputGear outputGear)
    {
        final ServiceProxy serviceProxy = factory.createServiceProxy(this, outputGear);

        inputGear.getDisruptor().handleEventsWith(serviceProxy);

        return this;
    }

    @Override
    public String toString()
    {
        return "H: mediaDriver=" + mediaDriver + " aeron=" + aeron + " replicator=" + replicator;
    }
}
