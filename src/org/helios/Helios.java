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
import org.helios.gateway.ServiceEventHandler;
import org.helios.gateway.ServiceEventHandlerFactory;
import org.helios.gateway.ServiceProxy;
import org.helios.gateway.ServiceProxyFactory;
import org.helios.mmb.*;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.AvailableImageHandler;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.UnavailableImageHandler;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class Helios implements AutoCloseable, ErrorHandler, AvailableImageHandler, UnavailableImageHandler
{
    private final HeliosContext context;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private Journaller inputJournaller;
    private Replicator replicator;
    private Consumer<Throwable> errorHandler;
    private AvailableAssociationHandler availableAssociationHandler;
    private UnavailableAssociationHandler unavailableAssociationHandler;

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

        final Aeron.Context aeronContext = new Aeron.Context().errorHandler(this).availableImageHandler(this).unavailableImageHandler(this);
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

    @Override
    public void onAvailableImage(final Image image)
    {
        if (availableAssociationHandler != null)
        {
            availableAssociationHandler.onAssociationEstablished();
        }
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        if (unavailableAssociationHandler != null)
        {
            unavailableAssociationHandler.onAssociationBroken();
        }
    }

    public Helios errorHandler(Consumer<Throwable> errorHandler)
    {
        this.errorHandler = errorHandler;
        return this;
    }

    public Helios availableAssociationHandler(final AvailableAssociationHandler handler)
    {
        this.availableAssociationHandler = handler;
        return this;
    }

    public Helios unavailableAssociationHandler(final UnavailableAssociationHandler handler)
    {
        this.unavailableAssociationHandler = handler;
        return this;
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

    public ServiceHandler addServiceHandler(final ServiceHandlerFactory factory, final InputGear inputGear, final OutputGear outputGear)
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

        return serviceHandler;
    }

    public <T extends ServiceProxy> T addServiceProxy(final ServiceProxyFactory<T> factory,
                                                      final String inputChannel, int inputStreamId,
                                                      final String outputChannel, int outputStreamId)
    {
        final AtomicBoolean running = new AtomicBoolean(true);

        final AeronSubscriber busSubscriber = new AeronSubscriber(running, aeron, inputChannel, inputStreamId);
        final AeronPublisher busPublisher = new AeronPublisher(aeron, outputChannel, outputStreamId);

        return factory.createServiceProxy(this, busSubscriber, busPublisher);
    }

    public ServiceEventHandler addServiceEventHandler(final ServiceEventHandlerFactory factory, final InputGear inputGear)
    {
        final ServiceEventHandler serviceEventHandler = factory.createServiceEventHandler(this, inputGear);

        inputGear.getDisruptor().handleEventsWith(serviceEventHandler);

        return serviceEventHandler;
    }

    @Override
    public String toString()
    {
        return "H: mediaDriver=" + mediaDriver + " aeron=" + aeron + " replicator=" + replicator;
    }
}
