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

public final class AMQP {

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQP.class);
    private final Meter amqpMeter;
    private final EventHubProducerClient producerClient;
    private final List<EventDataBatch> eventDataBatchList;

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
    }

    public void addEvents(final EventData eventData) {
        if (eventDataBatchList.isEmpty()) {
            eventDataBatchList.add(producerClient.createBatch());
        }
        // try to add the event to the batch
        if (!eventDataBatchList.getFirst().tryAdd(eventData)) {
            flushEvents();
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
        LOGGER.debug("Batch is full with <{}> events, sending it", eventDataBatchList.getFirst().getCount());
        // if the batch is full, send it and then create a new batch
        producerClient.send(eventDataBatchList.getFirst());
        LOGGER.info("Event batch sent successfully");
        amqpMeter.mark(eventDataBatchList.getFirst().getCount());
        eventDataBatchList.clear();
    }

    /**
     * Code for publishing events.
     * 
     * @throws IllegalArgumentException if the EventData is bigger than the max batch size.
     */
    public void publishEvents(final List<EventData> allEvents) {

        CreateBatchOptions options = new CreateBatchOptions();
        options.setMaximumSizeInBytes(1024);

        LOGGER.info("Publishing events to Event Hub with <{}> events", allEvents.size());
        // create a batch
        EventDataBatch eventDataBatch = producerClient.createBatch(options);
        for (final EventData eventData : allEvents) {
            // try to add the event from the array to the batch
            if (!eventDataBatch.tryAdd(eventData)) {
                LOGGER.debug("Batch is full with <{}> events, sending it", eventDataBatch.getCount());
                // if the batch is full, send it and then create a new batch
                producerClient.send(eventDataBatch);
                eventDataBatch = producerClient.createBatch(options);

                // Try to add that event that couldn't fit before.
                if (!eventDataBatch.tryAdd(eventData)) {
                    throw new IllegalArgumentException(
                            "Event is too large for an empty batch. Max size: " + eventDataBatch.getMaxSizeInBytes()
                    );
                }
            }
        }
        // send the last batch of remaining events
        if (eventDataBatch.getCount() > 0) {
            LOGGER.debug("Remaining event batch has <{}> events, sending it", eventDataBatch.getCount());
            producerClient.send(eventDataBatch);
        }

        LOGGER.info("Event batch sent successfully");
        amqpMeter.mark(allEvents.size());
    }

    public void close() {
        producerClient.close();
    }
}
