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

import io.aeron.*;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.Verify;
import org.helios.core.service.Service;
import org.helios.core.service.ServiceHandler;
import org.helios.core.service.ServiceHandlerFactory;
import org.helios.gateway.Gateway;
import org.helios.gateway.GatewayHandler;
import org.helios.gateway.GatewayHandlerFactory;
import org.helios.infra.RateReporter;
import org.helios.mmb.AvailableAssociationHandler;
import org.helios.mmb.UnavailableAssociationHandler;
import org.helios.util.ProcessorHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Helios implements AutoCloseable, ErrorHandler, AvailableImageHandler, UnavailableImageHandler
{
    public static final String IPC_CHANNEL = CommonContext.IPC_CHANNEL;

    private final HeliosContext context;
    private final HeliosDriver driver;
    private final Aeron aeron;
    private Consumer<Throwable> errorHandler;
    private AvailableAssociationHandler availableAssociationHandler;
    private UnavailableAssociationHandler unavailableAssociationHandler;
    private List<Service> serviceList;
    private List<Gateway<?>> gatewayList;
    private RateReporter reporter;

    public Helios(final HeliosContext context)
    {
        this(context, new HeliosDriver(context));
    }

    public Helios(final HeliosContext context, final HeliosDriver driver)
    {
        this.context = context;
        this.driver = driver;

        final Aeron.Context aeronContext = new Aeron.Context().errorHandler(this).availableImageHandler(this).unavailableImageHandler(this);
        if (context.isMediaDriverEmbedded())
        {
            aeronContext.aeronDirectoryName(driver.mediaDriver().aeronDirectoryName());
        }
        aeron = Aeron.connect(aeronContext);

        errorHandler = System.err::println;

        serviceList = new ArrayList<>();
        gatewayList = new ArrayList<>();

        reporter = context.isReportingEnabled() ? new RateReporter() : null;
    }

    public void start()
    {
        gatewayList.forEach(Gateway::start);
        serviceList.forEach(Service::start);

        ProcessorHelper.start(reporter);
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(reporter);

        for (Gateway<?> gw : gatewayList)
        {
            gw.close();
        }
        gatewayList.clear();

        for (Service svc : serviceList)
        {
            svc.close();
        }
        serviceList.clear();

        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(driver);
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

    public AeronStream newStream(final String channel, final int streamId)
    {
        Verify.notNull(channel, "channel");

        return new AeronStream(aeron, channel, streamId);
    }

    public <T extends ServiceHandler> Service<T> addEmbeddedService(
        final int reqStreamId, final int rspStreamId, final ServiceHandlerFactory<T> factory)
    {
        final AeronStream reqStream = new AeronStream(aeron, CommonContext.IPC_CHANNEL, reqStreamId);
        final AeronStream rspStream = new AeronStream(aeron, CommonContext.IPC_CHANNEL, rspStreamId);

        return addService(reqStream, rspStream, factory);
    }

    public <T extends ServiceHandler> Service<T> addService(final AeronStream reqStream, final AeronStream rspStream,
        final ServiceHandlerFactory<T> factory)
    {
        Verify.notNull(reqStream, "reqStream");
        Verify.notNull(rspStream, "rspStream");
        Verify.notNull(factory, "factory");

        final Service<T> svc = new HeliosService<>(context, reqStream, rspStream, factory);
        serviceList.add(svc);

        if (reporter != null)
        {
            reporter.add(svc.report());
        }

        return svc;
    }

    public <T extends GatewayHandler> Gateway<T> addEmbeddedGateway(
        final int reqStreamId, final int rspStreamId, final GatewayHandlerFactory<T> factory)
    {
        final AeronStream reqStream = new AeronStream(aeron, CommonContext.IPC_CHANNEL, reqStreamId);
        final AeronStream rspStream = new AeronStream(aeron, CommonContext.IPC_CHANNEL, rspStreamId);

        return addGateway(reqStream, rspStream, factory);
    }

    public <T extends GatewayHandler> Gateway<T> addGateway(final AeronStream reqStream, final AeronStream rspStream,
        final GatewayHandlerFactory<T> factory)
    {
        Verify.notNull(reqStream, "reqStream");
        Verify.notNull(rspStream, "rspStream");
        Verify.notNull(factory, "factory");

        final Gateway<T> gw = new HeliosGateway<>(context, reqStream, rspStream, factory);
        gatewayList.add(gw);

        if (reporter != null)
        {
            reporter.add(gw.report());
        }

        return gw;
    }
}
