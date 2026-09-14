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
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.azure.EventHubsEmulatorContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

final class AMQPTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPTest.class);

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

        // Create async consumer that listens for all incoming messages to EventHub.
        final List<String> receivedPayloads = new ArrayList<>();
        EventHubConsumerAsyncClient consumer = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildAsyncConsumerClient();
        consumer.receive(true).subscribe(event -> {
            receivedPayloads.add(event.getData().getBodyAsString());
        }, error -> {
            Assertions.fail("Error receiving events", error);
        }, () -> {
            LOGGER.info("Stream has ended");
        });

        final String connectionString = eventHubs.getConnectionString();
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
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
        Assertions.assertEquals(2, receivedPayloads.size());
        Assertions.assertTrue(receivedPayloads.contains("Test message one"));
        Assertions.assertTrue(receivedPayloads.contains("Test message two"));
        Assertions.assertEquals(2, amqpMeter.getCount());
        client.close();
        consumer.close();
    }

    @Test
    void testAddEventsMultiple() {

        // Create async consumer that listens for all incoming messages to EventHub.
        final List<String> receivedPayloads = new ArrayList<>();
        EventHubConsumerAsyncClient consumer = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildAsyncConsumerClient();
        consumer.receive(true).subscribe(event -> {
            receivedPayloads.add(event.getData().getBodyAsString());
        }, error -> {
            Assertions.fail("Error receiving events", error);
        }, () -> {
            LOGGER.info("Stream has ended");
        });

        final String connectionString = eventHubs.getConnectionString();

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
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

        // Waiting additional 10 seconds for async consumer client to receive the events for assertions...
        Assertions.assertDoesNotThrow(() -> Thread.sleep(10 * 1000));

        Assertions.assertEquals(1000, amqpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (EventData expectedEvent : expectedEvents) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedEvent.getBodyAsString()), "Message was not received by Eventhub: " + expectedEvent.getBodyAsString());
        }
        client.close();
        consumer.close();
    }

    @Test
    void testAmqpWithCredential() {
        final TokenCredential credential = new ManagedIdentityCredentialBuilder().clientId("testClientId").build();
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        final AMQP client = Assertions.assertDoesNotThrow(() -> new AMQP(credential, "eh1", "emulatorNs1", amqpMeter));
        // .publishEvents() is not supported by the EventHub Emulator when the client has been built using TokenCredential.
        client.close();
    }
}
