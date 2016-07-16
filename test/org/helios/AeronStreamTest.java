package org.helios;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.junit.Test;

public class AeronStreamTest
{
    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenAeronIsNull()
    {
        new AeronStream(null, "", 0);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenChannelIsNull()
    {
        final MediaDriver.Context driverContext = new MediaDriver.Context();
        driverContext.dirsDeleteOnStart(true);
        final MediaDriver driver = MediaDriver.launchEmbedded(driverContext);

        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.aeronDirectoryName(driver.aeronDirectoryName());

        new AeronStream(Aeron.connect(aeronContext), null, 0);
    }
}
