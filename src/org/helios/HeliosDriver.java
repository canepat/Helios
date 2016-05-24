package org.helios;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;

import java.io.Closeable;
import java.io.IOException;

public class HeliosDriver implements Closeable
{
    private final MediaDriver mediaDriver;

    public HeliosDriver(final HeliosContext context)
    {
        this(context, new MediaDriver.Context()
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
            .receiverIdleStrategy(new NoOpIdleStrategy())
            .senderIdleStrategy(new NoOpIdleStrategy()));
    }

    public HeliosDriver(final HeliosContext context, final MediaDriver.Context driverContext)
    {
        driverContext.dirsDeleteOnStart(true);

        String mediaDriverConf = context.getMediaDriverConf();
        if (mediaDriverConf != null)
        {
            MediaDriver.loadPropertiesFile(mediaDriverConf);
        }

        final boolean embeddedMediaDriver = context.isMediaDriverEmbedded();
        mediaDriver = embeddedMediaDriver ? MediaDriver.launchEmbedded(driverContext) : null;
    }

    @Override
    public void close() throws IOException
    {
        CloseHelper.quietClose(mediaDriver);
    }

    @Override
    public String toString()
    {
        return mediaDriver != null ? mediaDriver.toString() : null;
    }

    MediaDriver mediaDriver()
    {
        return mediaDriver;
    }
}
