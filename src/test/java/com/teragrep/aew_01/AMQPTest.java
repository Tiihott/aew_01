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
import com.azure.core.util.IterableStream;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.*;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.azure.EventHubsEmulatorContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

final class AMQPTest {

    static Network network;
    static AzuriteContainer azurite;
    static EventHubsEmulatorContainer eventHubs;

    @BeforeEach
    void setUp() {
        network = Network.newNetwork();
        azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:latest").withNetwork(network);
        azurite.start();
        eventHubs = new EventHubsEmulatorContainer("mcr.microsoft.com/azure-messaging/eventhubs-emulator:latest")
                .withConfig(MountableFile.forClasspathResource("eventhubs_config.json"))
                .acceptLicense()
                .withNetwork(network)
                .withAzuriteContainer(azurite);
        eventHubs.start();
    }

    @AfterEach
    void tearDown() {
        Assertions.assertDoesNotThrow(eventHubs::stop);
        Assertions.assertDoesNotThrow(azurite::stop);
        Assertions.assertDoesNotThrow(network::close);
    }

    @Test
    void testAddEvents() {
        final String connectionString = eventHubs.getConnectionString();

        // Create consumer client to assert that producer works as expected.
        final EventHubConsumerClient consumer = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildConsumerClient();
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP client = new AMQP(connectionString, "eh1", amqpMeter);
        final List<EventData> allEvents = Arrays
                .asList(new EventData("Test message one"), new EventData("Test message two"));
        for (EventData eventData : allEvents) {
            CompletableFuture<Boolean> acceptTransactionFuture = CompletableFuture.supplyAsync(() -> {
                return true;
            });
            client.addEvents(eventData, acceptTransactionFuture);
        }
        // Wait and .close() for the AMQP client to flush any remaining batches
        Assertions.assertDoesNotThrow(() -> Thread.sleep(10 * 1000));
        client.close();

        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from all partitions
        IterableStream<String> partitionIds = consumer.getPartitionIds();
        List<String> receivedPayloads = new ArrayList<>();
        partitionIds.forEach(partitionId -> {
            final IterableStream<PartitionEvent> events = consumer
                    .receiveFromPartition(partitionId, 2, startingPosition, Duration.ofSeconds(1));
            for (PartitionEvent event : events) {
                receivedPayloads.add(event.getData().getBodyAsString());
            }
        });
        Assertions.assertEquals(2, receivedPayloads.size());
        Assertions.assertTrue(receivedPayloads.contains("Test message one"));
        Assertions.assertTrue(receivedPayloads.contains("Test message two"));
        Assertions.assertEquals(2, amqpMeter.getCount());
        consumer.close();
    }

    @Test
    void testAddEventsMultiple() {
        final String connectionString = eventHubs.getConnectionString();

        // Create consumer client to assert that producer works as expected.
        final EventHubConsumerClient consumer = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildConsumerClient();
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP client = new AMQP(connectionString, "eh1", amqpMeter);
        final List<EventData> expectedEvents = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            CompletableFuture<Boolean> acceptTransactionFuture = CompletableFuture.supplyAsync(() -> {
                return true;
            });
            final EventData eventData = new EventData("Test message " + i);
            client.addEvents(eventData, acceptTransactionFuture);
            expectedEvents.add(eventData);
        }
        // Wait and .close() for the AMQP client to flush any remaining batches
        Assertions.assertDoesNotThrow(() -> Thread.sleep(10 * 1000));
        client.close();

        final String partitionId = "0";
        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from partition '0' and returns the first 1001 received or until the 30 seconds has elapsed.
        final IterableStream<PartitionEvent> events = consumer
                .receiveFromPartition(partitionId, 1001, startingPosition, Duration.ofSeconds(10));

        final Iterator<PartitionEvent> iterator = events.iterator();
        Assertions.assertTrue(iterator.hasNext());
        final List<EventData> resultEvents = new ArrayList<>();
        while (iterator.hasNext()) {
            PartitionEvent event = iterator.next();
            resultEvents.add(event.getData());
        }
        Assertions.assertEquals(1000, amqpMeter.getCount());
        for (EventData eventData : resultEvents) {
            Assertions.assertTrue(expectedEvents.contains(eventData));
        }
        consumer.close();
    }

    @Test
    void testAmqpWithCredential() {
        final TokenCredential credential = new ManagedIdentityCredentialBuilder().clientId("testClientId").build();
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP client = Assertions.assertDoesNotThrow(() -> new AMQP(credential, "eh1", "emulatorNs1", amqpMeter));
        // .publishEvents() is not supported by the EventHub Emulator when the client has been built using TokenCredential.
        client.close();
    }
}
