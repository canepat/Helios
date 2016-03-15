package org.helios.gateway;

import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.DirectBuffer;

public interface ServiceProxy extends AutoCloseable, FragmentHandler
{
    long send(final DirectBuffer buffer, final int length);

    long receive() throws Exception;

    void idle(final int workCount);
}
