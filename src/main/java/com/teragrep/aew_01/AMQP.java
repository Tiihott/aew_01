/*
 * RELP sink for Microsoft Azure EventHub (aew_01)
 * Copyright (C) 2021-2026 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.aew_01;

import com.azure.core.credential.TokenCredential;
import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class AMQP {

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQP.class);
    private final Meter amqpMeter;
    private final EventHubProducerClient producerClient;
    private final List<EventDataBatch> eventDataBatchList;
    private final static long MAX_BATCH_TIME_MS = 100;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final CreateBatchOptions options;

    // Connection using connectionString
    public AMQP(final String connectionString, final String eventHubName, Meter meter) {
        this.amqpMeter = meter;
        LOGGER
                .debug(
                        "Creating an EventHubProducerClient with Event Hub name <[{}]> and connection string <[{}]>",
                        eventHubName, connectionString
                );
        this.producerClient = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .buildProducerClient();
        this.eventDataBatchList = new ArrayList<>();
        this.options = new CreateBatchOptions();
        options.setMaximumSizeInBytes(1024);
    }

    // Connection using TokenCredential
    public AMQP(
            final TokenCredential credential,
            final String eventHubName,
            final String fullyQualifiedNamespace,
            Meter meter
    ) {
        this.amqpMeter = meter;
        LOGGER
                .debug(
                        "Creating an EventHubProducerClient with namespace <[{}]> and Event Hub name <[{}]>",
                        fullyQualifiedNamespace, eventHubName
                );
        this.producerClient = new EventHubClientBuilder()
                .fullyQualifiedNamespace(fullyQualifiedNamespace)
                .eventHubName(eventHubName)
                .credential(credential)
                .buildProducerClient();
        this.eventDataBatchList = new ArrayList<>();
        this.options = new CreateBatchOptions();
        options.setMaximumSizeInBytes(1024);
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::flushEvents, 0, MAX_BATCH_TIME_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    public void addEvents(final EventData eventData) {
        if (eventDataBatchList.isEmpty()) {
            eventDataBatchList.addFirst(producerClient.createBatch(options));
        }
        // try to add the event to the batch
        if (!eventDataBatchList.getFirst().tryAdd(eventData)) {
            eventDataBatchList.addFirst(producerClient.createBatch(options));
            // Try to add that event that couldn't fit before.
            if (!eventDataBatchList.getFirst().tryAdd(eventData)) {
                throw new IllegalArgumentException(
                        "Event is too large for an empty batch. Max size: "
                                + eventDataBatchList.getFirst().getMaxSizeInBytes()
                );
            }
        }
    }

    public void flushEvents() {
        while (!eventDataBatchList.isEmpty() && eventDataBatchList.getLast().getCount() > 0) {
            if (eventDataBatchList.getLast().getCount() > 0) {
                LOGGER
                        .debug(
                                "Batch is ready to be sent with <{}> events, sending it",
                                eventDataBatchList.getLast().getCount()
                        );
                final EventDataBatch eventDataBatch = eventDataBatchList.removeLast();
                producerClient.send(eventDataBatch);
                LOGGER.info("Event batch sent successfully");
                amqpMeter.mark(eventDataBatch.getCount());
            }
        }
        if (eventDataBatchList.isEmpty()) {
            eventDataBatchList.addFirst(producerClient.createBatch(options));
        }
    }

    public void close() {
        producerClient.close();
    }
}
