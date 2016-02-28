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

import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.helios.core.engine.InputBufferEvent;
import org.helios.core.engine.OutputBufferEvent;
import org.helios.core.engine.ServiceHandler;
import org.helios.core.engine.ServiceHandlerFactory;
import org.helios.core.journal.Journaller;
import org.helios.core.journal.strategy.JournalStrategy;
import org.helios.core.journal.strategy.PositionalWriteJournalStrategy;
import org.helios.core.journal.strategy.SeekThenWriteJournalStrategy;
import org.helios.core.replica.Replicator;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Helios implements AutoCloseable, ErrorHandler
{
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Journaller inputJournaller;
    private Replicator replicator;

    public Helios()
    {
        this(new MediaDriver.Context()
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
            .receiverIdleStrategy(new NoOpIdleStrategy())
            .senderIdleStrategy(new NoOpIdleStrategy()));
    }

    public Helios(final MediaDriver.Context driverContext)
    {
        String mediaDriverConf = System.getProperty("helios.core.media_driver.conf");
        if (mediaDriverConf != null)
        {
            MediaDriver.loadPropertiesFile(mediaDriverConf);
        }

        boolean embeddedMediaDriver = !Boolean.getBoolean("helios.core.media_driver.external");
        mediaDriver = embeddedMediaDriver ? MediaDriver.launchEmbedded(driverContext) : null;

        Aeron.Context aeronContext = new Aeron.Context().errorHandler(this);
        if (embeddedMediaDriver)
        {
            aeronContext.aeronDirectoryName(mediaDriver.aeronDirectoryName());
        }
        aeron = Aeron.connect(aeronContext);

        final String journalDirName = System.getProperty("helios.core.journal.dir", "./");
        final int journalFileSize = Integer.getInteger("helios.core.journal.file_size", 1024 * 1024 * 1024);
        final int journalFileCount = Integer.getInteger("helios.core.journal.file_count", 1);
        boolean seekAndWrite = Boolean.getBoolean("helios.core.journal.seek_and_write");
        boolean flushing = Boolean.getBoolean("helios.core.journal.flushing");

        final Path journalDir = Paths.get(journalDirName);
        final JournalStrategy journalStrategy = seekAndWrite ?
            new SeekThenWriteJournalStrategy(journalDir, journalFileSize, journalFileCount) :
            new PositionalWriteJournalStrategy(journalDir, journalFileSize, journalFileCount);

        inputJournaller = new Journaller(journalStrategy, flushing);

        boolean replicatorEnabled = Boolean.getBoolean("helios.core.replicator.enabled");
        if (replicatorEnabled)
        {
            replicator = new Replicator(aeron);
        }
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
        //System.err.println(throwable); //TODO: logging
    }

    public InputGear addInputGear(int bufferSize, final WaitStrategy strategy, final String channel, int streamId)
    {
        final ThreadFactory threadFactory = Executors.privilegedThreadFactory();
        final Disruptor<InputBufferEvent> disruptor = new Disruptor<>(InputBufferEvent::new, bufferSize, threadFactory, ProducerType.SINGLE, strategy);

        return new InputGear(disruptor, aeron, channel, streamId);
    }

    public OutputGear addOutputGear(int bufferSize, final WaitStrategy strategy, final String channel, int streamId)
    {
        final ThreadFactory threadFactory = Executors.privilegedThreadFactory();
        final Disruptor<OutputBufferEvent> disruptor = new Disruptor<>(OutputBufferEvent::new, bufferSize, threadFactory, ProducerType.SINGLE, strategy);

        return new OutputGear(disruptor, aeron, channel, streamId);
    }

    public Helios addServiceHandler(final ServiceHandlerFactory factory, final InputGear inputGear, final OutputGear outputGear)
    {
        final ServiceHandler serviceHandler = factory.createServiceHandler(this, outputGear);

        if (replicator != null)
        {
            inputGear.getDisruptor().handleEventsWith(inputJournaller, replicator).then(serviceHandler);
        }
        else
        {
            inputGear.getDisruptor().handleEventsWith(inputJournaller).then(serviceHandler);
        }

        return this;
    }

    @Override
    public String toString()
    {
        return "H: mediaDriver=" + mediaDriver + " aeron=" + aeron + " replicator=" + replicator;
    }
}
