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
import org.agrona.collections.Long2ObjectHashMap;
import org.helios.gateway.Gateway;
import org.helios.gateway.GatewayHandler;
import org.helios.gateway.GatewayHandlerFactory;
import org.helios.infra.AvailableAssociationHandler;
import org.helios.infra.ReportProcessor;
import org.helios.infra.UnavailableAssociationHandler;
import org.helios.service.Service;
import org.helios.service.ServiceHandler;
import org.helios.service.ServiceHandlerFactory;
import org.helios.util.ProcessorHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class Helios implements AutoCloseable, ErrorHandler, AvailableImageHandler, UnavailableImageHandler
{
    private final HeliosContext context;
    private final Aeron aeron;
    private final HeliosDriver driver;
    private Consumer<Throwable> errorHandler;
    private final List<HeliosService<?>> serviceList;
    private final List<HeliosGateway<?>> gatewayList;
    private final Long2ObjectHashMap<HeliosService<?>> serviceSubscriptionRepository;
    private final Long2ObjectHashMap<HeliosGateway<?>> gatewaySubscriptionRepository;

    private ReportProcessor reporter;

    public Helios()
    {
        this(new HeliosContext());
    }

    public Helios(final HeliosContext context)
    {
        this(context, new HeliosDriver(context));
    }

    public Helios(final HeliosContext context, final HeliosDriver driver)
    {
        this.context = context;
        this.driver = driver;

        final Aeron.Context aeronContext = new Aeron.Context()
            .errorHandler(this).availableImageHandler(this).unavailableImageHandler(this);

        if (context.isMediaDriverEmbedded())
        {
            aeronContext.aeronDirectoryName(driver.mediaDriver().aeronDirectoryName());
        }
        aeron = Aeron.connect(aeronContext);

        errorHandler = System.err::println;

        serviceList = new ArrayList<>();
        gatewayList = new ArrayList<>();
        serviceSubscriptionRepository = new Long2ObjectHashMap<>();
        gatewaySubscriptionRepository = new Long2ObjectHashMap<>();

        reporter = context.isReportingEnabled() ? new ReportProcessor(1_000_000_000L, null) : null; // TODO: configure
    }

    public void start()
    {
        gatewayList.forEach(Gateway::start);
        serviceList.forEach(Service::start);

        ProcessorHelper.start(reporter);
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(reporter);

        gatewayList.forEach(CloseHelper::quietClose);
        gatewayList.clear();
        gatewaySubscriptionRepository.clear();

        serviceList.forEach(CloseHelper::quietClose);
        serviceList.clear();
        serviceSubscriptionRepository.clear();

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
        final long subscriptionId = image.subscription().registrationId();

        final HeliosService<?> svc = serviceSubscriptionRepository.get(subscriptionId);
        if (svc != null)
        {
            svc.onAvailableImage(image);
        }

        final HeliosGateway<?> gw = gatewaySubscriptionRepository.get(subscriptionId);
        if (gw != null)
        {
            gw.onAvailableImage(image);
        }
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        final long subscriptionId = image.subscription().registrationId();

        final HeliosService<?> svc = serviceSubscriptionRepository.get(subscriptionId);
        if (svc != null)
        {
            svc.onUnavailableImage(image);
        }

        final HeliosGateway<?> gw = gatewaySubscriptionRepository.get(subscriptionId);
        if (gw != null)
        {
            gw.onUnavailableImage(image);
        }
    }

    public Helios errorHandler(Consumer<Throwable> errorHandler)
    {
        this.errorHandler = Objects.requireNonNull(errorHandler);
        return this;
    }

    public AeronStream newStream(final String channel, final int streamId)
    {
        return new AeronStream(aeron, channel, streamId);
    }

    public AeronStream newIpcStream(final int streamId)
    {
        return newStream(CommonContext.IPC_CHANNEL, streamId);
    }

    public <T extends ServiceHandler> Service<T> addService(final ServiceHandlerFactory<T> factory, final AeronStream reqStream)
    {
        Objects.requireNonNull(factory, "factory");
        Objects.requireNonNull(reqStream, "reqStream");

        final HeliosService<T> svc = new HeliosService<>(this, factory, reqStream);
        serviceList.add(svc);

        if (reporter != null)
        {
            reporter.add(svc.report());
        }

        return svc;
    }

    public <T extends ServiceHandler> Service<T> addService(final ServiceHandlerFactory<T> factory,
        final AeronStream reqStream, final AeronStream rspStream)
    {
        final Service<T> svc = addService(factory, reqStream);

        return svc.addEndPoint(rspStream);
    }

    public <T extends ServiceHandler> Service<T> addService(final ServiceHandlerFactory<T> factory, final AeronStream reqStream,
        final AvailableAssociationHandler availableHandler, final UnavailableAssociationHandler unavailableHandler)
    {
        final Service<T> svc = addService(factory, reqStream);

        svc.availableAssociationHandler(availableHandler).unavailableAssociationHandler(unavailableHandler);

        return svc;
    }

    public <T extends ServiceHandler> Service<T> addService(final ServiceHandlerFactory<T> factory,
        final AvailableAssociationHandler availableHandler, final UnavailableAssociationHandler unavailableHandler,
        final AeronStream reqStream, final AeronStream rspStream)
    {
        final Service<T> svc = addService(factory, reqStream, availableHandler, unavailableHandler);

        return svc.addEndPoint(rspStream);
    }

    public <T extends GatewayHandler> Gateway<T> addGateway()
    {
        final HeliosGateway<T> gw = new HeliosGateway<>(this);
        gatewayList.add(gw);

        if (reporter != null)
        {
            reporter.add(gw.report());
        }

        return gw;
    }

    public <T extends GatewayHandler> T addGateway(final GatewayHandlerFactory<T> factory,
        final AeronStream reqStream, final AeronStream rspStream)
    {
        final Gateway<T> gw = addGateway();

        return gw.addEndPoint(reqStream, rspStream, factory);
    }

    public <T extends GatewayHandler> Gateway<T> addGateway(final AvailableAssociationHandler availableHandler,
        final UnavailableAssociationHandler unavailableHandler)
    {
        final Gateway<T> gw = addGateway();

        gw.availableAssociationHandler(availableHandler).unavailableAssociationHandler(unavailableHandler);

        return gw;
    }

    public <T extends GatewayHandler> T addGateway(final GatewayHandlerFactory<T> factory,
        final AvailableAssociationHandler availableHandler, final UnavailableAssociationHandler unavailableHandler,
        final AeronStream reqStream, final AeronStream rspStream)
    {
        final Gateway<T> gw = addGateway(availableHandler, unavailableHandler);

        return gw.addEndPoint(reqStream, rspStream, factory);
    }

    HeliosContext context()
    {
        return context;
    }

    Aeron aeron()
    {
        return aeron;
    }

    <T extends ServiceHandler> void addServiceSubscription(final long subscriptionId, final HeliosService<T> svc)
    {
        serviceSubscriptionRepository.put(subscriptionId, svc);
    }

    <T extends GatewayHandler> void addGatewaySubscription(final long subscriptionId, final HeliosGateway<T> gw)
    {
        gatewaySubscriptionRepository.put(subscriptionId, gw);
    }

    int numServiceSubscriptions()
    {
        return serviceSubscriptionRepository.size();
    }

    int numGatewaySubscriptions()
    {
        return gatewaySubscriptionRepository.size();
    }
}
