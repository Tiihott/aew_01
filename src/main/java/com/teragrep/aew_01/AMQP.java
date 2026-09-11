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
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class AMQP {

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQP.class);
    private final Meter amqpMeter;
    private final EventHubProducerAsyncClient producerClient;
    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

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
                .buildAsyncProducerClient();
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
                .buildAsyncProducerClient();
    }

    public CompletableFuture<Void> addEvents(final EventData eventData, CompletableFuture<Boolean> futureAck) {
        CompletableFuture<Void> future = producerClient.createBatch().flatMap(batch -> {
            if (!batch.tryAdd(eventData)) {
                throw new RuntimeException("Something went wrong with adding events to batch.");
            }
            return producerClient.send(batch);
        }).toFuture().whenCompleteAsync((Void, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error occurred publishing event: {}", throwable.getMessage());
            }
            else {
                try {
                    boolean success = futureAck.get();
                    if (success) {
                        amqpMeter.mark();
                        LOGGER.debug("Successfully published event");
                    }
                    else {
                        LOGGER.error("Error occurred publishing event");
                    }
                }
                catch (InterruptedException e) {
                    LOGGER.error("Error occurred publishing event: ", e);
                    throw new RuntimeException(e);
                }
                catch (ExecutionException e) {
                    LOGGER.error("Error occurred publishing event: ", e);
                    throw new RuntimeException(e);
                }
            }
        }, virtualThreadExecutor);
        return future;
    }

    public void close() {
        producerClient.close();
    }
}
