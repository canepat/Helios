package org.helios.service;

import org.agrona.concurrent.MessageHandler;

public interface ServiceHandler extends MessageHandler, AutoCloseable
{
}
