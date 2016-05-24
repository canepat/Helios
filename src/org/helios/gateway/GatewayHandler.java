package org.helios.gateway;

import org.agrona.concurrent.MessageHandler;

public interface GatewayHandler extends MessageHandler, AutoCloseable
{
}
