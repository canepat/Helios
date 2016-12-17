package org.helios;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;

import java.io.Closeable;

public class HeliosDriver implements Closeable
{
    private final static String AERON_DIR_NAME_DEFAULT = "./.aeron";

    private final MediaDriver mediaDriver;

    public HeliosDriver(final HeliosContext context)
    {
        this(context, AERON_DIR_NAME_DEFAULT);
    }

    public HeliosDriver(final HeliosContext context, final String aeronDirectoryName)
    {
        this(context, new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(false)
            .threadingMode(ThreadingMode.SHARED)
            .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
            .receiverIdleStrategy(new NoOpIdleStrategy())
            .senderIdleStrategy(new NoOpIdleStrategy()));
    }

    public HeliosDriver(final HeliosContext context, final MediaDriver.Context driverContext)
    {
        String mediaDriverConf = context.getMediaDriverConf();
        if (mediaDriverConf != null)
        {
            MediaDriver.loadPropertiesFile(mediaDriverConf);
        }
        else
        {
            driverContext.dirsDeleteOnStart(true);
        }

        driverContext.warnIfDirectoriesExist(false);

        final boolean embeddedMediaDriver = context.isMediaDriverEmbedded();
        mediaDriver = embeddedMediaDriver ? MediaDriver.launchEmbedded(driverContext) : null;
    }

    @Override
    public void close()
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
