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

import com.azure.core.util.IterableStream;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.teragrep.rlp_01.RelpBatch;
import com.teragrep.rlp_01.RelpCommand;
import com.teragrep.rlp_01.RelpConnection;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.delegate.event.RelpEvent;
import com.teragrep.rlp_03.frame.delegate.event.RelpEventClose;
import com.teragrep.rlp_03.frame.delegate.event.RelpEventOpen;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.azure.EventHubsEmulatorContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class IntegrationDeferredTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationDeferredTest.class);

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
    void testDeferredRelpAndAmqp() {
        /*
         * DefaultFrameDelegate accepts Map<String, RelpEvent> for processing of the commands
         */

        Map<String, RelpEvent> relpCommandConsumerMap = new HashMap<>();
        /*
         * Add default commands, open and close, they are mandatory
         */
        relpCommandConsumerMap.put(RelpCommand.OPEN, new RelpEventOpen());
        relpCommandConsumerMap.put(RelpCommand.CLOSE, new RelpEventClose());

        /*
         * Queue for deferring the processing of the frames
         */
        BlockingQueue<FrameContext> frameContexts = new ArrayBlockingQueue<>(1024);
        RelpEvent syslogRelpEvent = new RelpEvent() {

            @Override
            public void accept(FrameContext frameContext) {
                frameContexts.add(frameContext);
            }

            @Override
            public void close() {
                frameContexts.clear();
            }
        };

        relpCommandConsumerMap.put(RelpCommand.SYSLOG, syslogRelpEvent);

        final String connectionString = eventHubs.getConnectionString();

        // Create consumer client to assert that producer works as expected.
        final EventHubConsumerClient eventHubConsumerClient = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildConsumerClient();

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        Meter relpMeter = metricRegistry.meter("relpMeter");
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP amqpClient = new AMQP(connectionString, "eh1", amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, relpMeter);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send 2 messages to the RELP server.
        final RelpConnection relpConnection = new RelpConnection();
        final int port = 1601;
        Assertions.assertDoesNotThrow(() -> relpConnection.connect("localhost", port));
        final RelpBatch relpBatch = new RelpBatch();
        List<String> expectedPayloads = new ArrayList<>();
        expectedPayloads.add("Hello World! 1");
        expectedPayloads.add("Hello World! 2");
        long reqId1 = relpBatch.insert("Hello World! 1".getBytes(StandardCharsets.UTF_8));
        long reqId2 = relpBatch.insert("Hello World! 2".getBytes(StandardCharsets.UTF_8));
        Assertions.assertAll(() -> relpConnection.commit(relpBatch));
        // Wait for the AMQP scheduler to flush any remaining batches and close.
        while (amqpMeter.getCount() < 2) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        amqpClient.close();
        // verify successful transaction
        Assertions.assertTrue(relpBatch.verifyTransaction(reqId1));
        Assertions.assertTrue(relpBatch.verifyTransaction(reqId2));
        Assertions.assertAll(relpConnection::disconnect);
        relp.close();

        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from all partitions
        IterableStream<String> partitionIds = eventHubConsumerClient.getPartitionIds();
        List<String> receivedPayloads = new ArrayList<>();
        partitionIds.forEach(partitionId -> {
            final IterableStream<PartitionEvent> events = eventHubConsumerClient
                    .receiveFromPartition(partitionId, 2, startingPosition, Duration.ofMillis(100));
            for (PartitionEvent event : events) {
                receivedPayloads.add(event.getData().getBodyAsString());
            }
        });
        Assertions.assertEquals(expectedPayloads.size(), receivedPayloads.size());
        Assertions.assertTrue(expectedPayloads.contains(receivedPayloads.get(0)));
        Assertions.assertTrue(expectedPayloads.contains(receivedPayloads.get(1)));
        Assertions.assertEquals(2, amqpMeter.getCount());
        eventHubConsumerClient.close();
        /*
         * Stop the deferred processing thread
         */
        deferredSyslog.run.set(false);
        try {
            deferredProcessingThread.join();
        }
        catch (InterruptedException interruptedException) {
            throw new RuntimeException(interruptedException);
        }
    }

    @Test
    void testDeferredRelpAndAmqpSingleMediumBatch() {
        /*
         * DefaultFrameDelegate accepts Map<String, RelpEvent> for processing of the commands
         */

        Map<String, RelpEvent> relpCommandConsumerMap = new HashMap<>();
        /*
         * Add default commands, open and close, they are mandatory
         */
        relpCommandConsumerMap.put(RelpCommand.OPEN, new RelpEventOpen());
        relpCommandConsumerMap.put(RelpCommand.CLOSE, new RelpEventClose());

        /*
         * Queue for deferring the processing of the frames
         */
        BlockingQueue<FrameContext> frameContexts = new ArrayBlockingQueue<>(1024);
        RelpEvent syslogRelpEvent = new RelpEvent() {

            @Override
            public void accept(FrameContext frameContext) {
                frameContexts.add(frameContext);
            }

            @Override
            public void close() {
                frameContexts.clear();
            }
        };

        relpCommandConsumerMap.put(RelpCommand.SYSLOG, syslogRelpEvent);

        final String connectionString = eventHubs.getConnectionString();

        // Create consumer client to assert that producer works as expected.
        final EventHubConsumerClient eventHubConsumerClient = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildConsumerClient();

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        Meter relpMeter = metricRegistry.meter("relpMeter");
        final AMQP amqpClient = new AMQP(connectionString, "eh1", amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, relpMeter);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send batch of 1000 messages to the RELP server.
        final RelpConnection relpConnection = new RelpConnection();
        final int port = 1601;
        Assertions.assertDoesNotThrow(() -> relpConnection.connect("localhost", port));
        final RelpBatch relpBatch = new RelpBatch();
        List<Long> reqIds = new ArrayList<>();
        List<String> expectedPayloads = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            String payload = "Hello World " + i;
            reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
            expectedPayloads.add(payload);
        }

        Assertions.assertAll(() -> relpConnection.commit(relpBatch));
        // Wait for the AMQP scheduler to flush any remaining batches and close.
        while (amqpMeter.getCount() < 1000) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        amqpClient.close();
        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }

        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from all partitions
        IterableStream<String> partitionIds = eventHubConsumerClient.getPartitionIds();
        List<String> receivedPayloads = new ArrayList<>();
        partitionIds.stream().forEach(partitionId -> {
            final IterableStream<PartitionEvent> events = eventHubConsumerClient
                    .receiveFromPartition(partitionId, 1000, startingPosition, Duration.ofMillis(200));
            for (PartitionEvent event : events) {
                receivedPayloads.add(event.getData().getBodyAsString());
            }
        });
        Assertions.assertEquals(expectedPayloads.size(), receivedPayloads.size());
        Assertions.assertEquals(1000, amqpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
        Assertions.assertAll(relpConnection::disconnect);
        relp.close();
        eventHubConsumerClient.close();
        /*
         * Stop the deferred processing thread
         */
        deferredSyslog.run.set(false);
        try {
            deferredProcessingThread.join();
        }
        catch (InterruptedException interruptedException) {
            throw new RuntimeException(interruptedException);
        }
    }

    @EnabledIfSystemProperty(
            named = "runHeavyTests",
            matches = "true"
    )
    @Test
    void testDeferredRelpAndAmqpMultipleMediumBatches() {
        /*
         * DefaultFrameDelegate accepts Map<String, RelpEvent> for processing of the commands
         */

        Map<String, RelpEvent> relpCommandConsumerMap = new HashMap<>();
        /*
         * Add default commands, open and close, they are mandatory
         */
        relpCommandConsumerMap.put(RelpCommand.OPEN, new RelpEventOpen());
        relpCommandConsumerMap.put(RelpCommand.CLOSE, new RelpEventClose());

        /*
         * Queue for deferring the processing of the frames
         */
        BlockingQueue<FrameContext> frameContexts = new ArrayBlockingQueue<>(10024);
        RelpEvent syslogRelpEvent = new RelpEvent() {

            @Override
            public void accept(FrameContext frameContext) {
                frameContexts.add(frameContext);
            }

            @Override
            public void close() {
                frameContexts.clear();
            }
        };

        relpCommandConsumerMap.put(RelpCommand.SYSLOG, syslogRelpEvent);

        final String connectionString = eventHubs.getConnectionString();

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        Meter relpMeter = metricRegistry.meter("relpMeter");
        final AMQP amqpClient = new AMQP(connectionString, "eh1", amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, relpMeter);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send 10 batches of 1000 messages to the RELP server.
        final int port = 1601;
        int cursor = 1;
        final List<String> expectedPayloads = new ArrayList<>();
        List<Thread> sendThreads = new LinkedList<>();
        for (int j = 1; j <= 2; j++) {
            final RelpBatch relpBatch = new RelpBatch();
            final List<Long> reqIds = new ArrayList<>();
            for (int i = cursor; i < cursor + 1000; i++) {
                String payload = "Hello World" + i;
                reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
                expectedPayloads.add(payload);
            }
            sendThreads.add(sendBatch(port, relpBatch));
            cursor += 1000;
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000)); // FIXME: Drops events without sleep, relpMeter/amqpMeter does not reach expected amounts.
        }
        // Wait for the AMQP scheduler to flush events to eventhub
        while (amqpMeter.getCount() < 2000) {
            LOGGER.info("Waiting for events to be received AMQP... " + amqpMeter.getCount() + "/2000");
            LOGGER.info("Waiting for events to be received RELP... " + relpMeter.getCount() + "/2000");
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        amqpClient.close();
        relp.close();

        // Create consumer client to assert that producer worked as expected.
        final EventHubConsumerClient eventHubConsumerClient = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildConsumerClient();
        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from all partitions
        final IterableStream<String> partitionIds = eventHubConsumerClient.getPartitionIds();
        final List<String> receivedPayloads = new ArrayList<>();
        for (final String partitionId : partitionIds) {
            final IterableStream<PartitionEvent> events = eventHubConsumerClient
                    .receiveFromPartition(partitionId, 1000, startingPosition, Duration.ofMillis(200));
            for (final PartitionEvent event : events) {
                receivedPayloads.add(event.getData().getBodyAsString());
            }
        }
        Assertions.assertEquals(expectedPayloads.size(), receivedPayloads.size());
        Assertions.assertEquals(2000, amqpMeter.getCount());
        Assertions.assertEquals(2000, relpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (final String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
        eventHubConsumerClient.close();
        /*
         * Stop the deferred processing thread
         */
        deferredSyslog.run.set(false);

        Assertions.assertDoesNotThrow(() -> deferredProcessingThread.join());

        amqpClient.close();
        relp.close();
    }

    @EnabledIfSystemProperty(
            named = "runHeavyTests",
            matches = "true"
    )
    @Test
    void testDeferredRelpAndAmqp100x100Batches() {
        /*
         * DefaultFrameDelegate accepts Map<String, RelpEvent> for processing of the commands
         */

        Map<String, RelpEvent> relpCommandConsumerMap = new HashMap<>();
        /*
         * Add default commands, open and close, they are mandatory
         */
        relpCommandConsumerMap.put(RelpCommand.OPEN, new RelpEventOpen());
        relpCommandConsumerMap.put(RelpCommand.CLOSE, new RelpEventClose());

        /*
         * Queue for deferring the processing of the frames
         */
        BlockingQueue<FrameContext> frameContexts = new ArrayBlockingQueue<>(10024);
        RelpEvent syslogRelpEvent = new RelpEvent() {

            @Override
            public void accept(FrameContext frameContext) {
                frameContexts.add(frameContext);
            }

            @Override
            public void close() {
                frameContexts.clear();
            }
        };

        relpCommandConsumerMap.put(RelpCommand.SYSLOG, syslogRelpEvent);

        final String connectionString = eventHubs.getConnectionString();

        // Create consumer client to assert that producer works as expected.
        final EventHubConsumerClient eventHubConsumerClient = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildConsumerClient();

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        Meter relpMeter = metricRegistry.meter("relpMeter");
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP amqpClient = new AMQP(connectionString, "eh1", amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, relpMeter);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send 100 batches of 100 messages to the RELP server.
        final int port = 1601;
        int cursor = 1;
        final List<String> expectedPayloads = new ArrayList<>();
        List<Thread> sendThreads = new LinkedList<>();
        for (int j = 1; j <= 100; j++) {
            final RelpBatch relpBatch = new RelpBatch();
            final List<Long> reqIds = new ArrayList<>();
            for (int i = cursor; i < cursor + 100; i++) {
                String payload = "Hello World" + i;
                reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
                expectedPayloads.add(payload);
            }
            sendThreads.add(sendBatch(port, relpBatch));
            cursor += 100;
            Assertions.assertDoesNotThrow(() -> Thread.sleep(100));
        }
        // Wait for the AMQP scheduler to flush events to eventhub
        while (amqpMeter.getCount() < 10000) {
            LOGGER.info("Waiting for events to be received AMQP... " + amqpMeter.getCount() + "/10000");
            LOGGER.info("Waiting for events to be received RELP... " + relpMeter.getCount() + "/10000");
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        amqpClient.close();
        relp.close();

        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from all partitions
        IterableStream<String> partitionIds = eventHubConsumerClient.getPartitionIds();
        List<String> receivedPayloads = new ArrayList<>();
        partitionIds.forEach(partitionId -> {
            final IterableStream<PartitionEvent> events = eventHubConsumerClient
                    .receiveFromPartition(partitionId, 1000, startingPosition, Duration.ofMillis(200));
            for (PartitionEvent event : events) {
                receivedPayloads.add(event.getData().getBodyAsString());
            }
        });
        Assertions.assertEquals(expectedPayloads.size(), receivedPayloads.size());
        Assertions.assertEquals(10000, amqpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
        eventHubConsumerClient.close();
        /*
         * Stop the deferred processing thread
         */
        deferredSyslog.run.set(false);
        try {
            deferredProcessingThread.join();
        }
        catch (InterruptedException interruptedException) {
            throw new RuntimeException(interruptedException);
        }
    }

    @EnabledIfSystemProperty(
            named = "runHeavyTests",
            matches = "true"
    )
    @Test
    void testDeferredRelpAndAmqpSingleLargeBatch() {
        /*
         * DefaultFrameDelegate accepts Map<String, RelpEvent> for processing of the commands
         */

        Map<String, RelpEvent> relpCommandConsumerMap = new HashMap<>();
        /*
         * Add default commands, open and close, they are mandatory
         */
        relpCommandConsumerMap.put(RelpCommand.OPEN, new RelpEventOpen());
        relpCommandConsumerMap.put(RelpCommand.CLOSE, new RelpEventClose());

        /*
         * Queue for deferring the processing of the frames
         */
        BlockingQueue<FrameContext> frameContexts = new ArrayBlockingQueue<>(10024);
        RelpEvent syslogRelpEvent = new RelpEvent() {

            @Override
            public void accept(FrameContext frameContext) {
                frameContexts.add(frameContext);
            }

            @Override
            public void close() {
                frameContexts.clear();
            }
        };

        relpCommandConsumerMap.put(RelpCommand.SYSLOG, syslogRelpEvent);

        final String connectionString = eventHubs.getConnectionString();

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        Meter relpMeter = metricRegistry.meter("relpMeter");
        final AMQP amqpClient = new AMQP(connectionString, "eh1", amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, relpMeter);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send batch of 10000 messages to the RELP server.
        final int port = 1601;
        final RelpBatch relpBatch = new RelpBatch();
        List<Long> reqIds = new ArrayList<>();
        List<String> expectedPayloads = new ArrayList<>();
        for (int i = 1; i <= 10000; i++) {
            String payload = "Hello World " + i;
            reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
            expectedPayloads.add(payload);
        }
        List<Thread> sendThreads = new LinkedList<>();
        sendThreads.add(sendBatch(port, relpBatch));
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }

        // Wait for the AMQP scheduler to flush events to eventhub
        while (amqpMeter.getCount() < 10000) {
            LOGGER.info("Waiting for events to be received... {}/10000", amqpMeter.getCount());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }
        relp.close();
        amqpClient.close();

        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from all partitions
        // Create consumer client to assert that producer works as expected.
        List<String> receivedPayloads = new ArrayList<>();
        final EventHubConsumerClient eventHubConsumerClient = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildConsumerClient();
        IterableStream<String> partitionIds = eventHubConsumerClient.getPartitionIds();
        partitionIds.forEach(partitionId -> {
            final IterableStream<PartitionEvent> events = eventHubConsumerClient
                    .receiveFromPartition(partitionId, 1000, startingPosition, Duration.ofMillis(500));
            for (PartitionEvent event : events) {
                receivedPayloads.add(event.getData().getBodyAsString());
            }
        });
        Assertions.assertEquals(expectedPayloads.size(), receivedPayloads.size());
        Assertions.assertEquals(10000, amqpMeter.getCount());
        Assertions.assertEquals(10000, relpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
        eventHubConsumerClient.close();
        /*
         * Stop the deferred processing thread
         */
        deferredSyslog.run.set(false);
        try {
            deferredProcessingThread.join();
        }
        catch (InterruptedException interruptedException) {
            throw new RuntimeException(interruptedException);
        }
    }

    @EnabledIfSystemProperty(
            named = "runVeryHeavyTests",
            matches = "true"
    )
    @Test
    void testDeferredRelpAndAmqpSingleVeryLargeBatch() {

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

        /*
         * DefaultFrameDelegate accepts Map<String, RelpEvent> for processing of the commands
         */

        Map<String, RelpEvent> relpCommandConsumerMap = new HashMap<>();
        /*
         * Add default commands, open and close, they are mandatory
         */
        relpCommandConsumerMap.put(RelpCommand.OPEN, new RelpEventOpen());
        relpCommandConsumerMap.put(RelpCommand.CLOSE, new RelpEventClose());

        /*
         * Queue for deferring the processing of the frames
         */
        BlockingQueue<FrameContext> frameContexts = new ArrayBlockingQueue<>(100024);
        RelpEvent syslogRelpEvent = new RelpEvent() {

            @Override
            public void accept(FrameContext frameContext) {
                frameContexts.add(frameContext);
            }

            @Override
            public void close() {
                frameContexts.clear();
            }
        };

        relpCommandConsumerMap.put(RelpCommand.SYSLOG, syslogRelpEvent);

        final String connectionString = eventHubs.getConnectionString();

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        Meter relpMeter = metricRegistry.meter("relpMeter");
        final AMQP amqpClient = new AMQP(connectionString, "eh1", amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, relpMeter);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send batch of 100000 messages to the RELP server.
        final int port = 1601;
        final RelpBatch relpBatch = new RelpBatch();
        List<Long> reqIds = new ArrayList<>();
        List<String> expectedPayloads = new ArrayList<>();
        for (int i = 1; i <= 100000; i++) {
            String payload = "Hello World " + i;
            reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
            expectedPayloads.add(payload);
        }
        List<Thread> sendThreads = new LinkedList<>();
        sendThreads.add(sendBatch(port, relpBatch));
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        // Wait for the AMQP to flush all events to eventhub
        while (amqpMeter.getCount() < 100000 || receivedPayloads.size() < 100000) {
            LOGGER.info("Waiting for events to be received AMQP... {}/100000", amqpMeter.getCount());
            LOGGER.info("Waiting for events to be received RELP... {}/100000", relpMeter.getCount());
            LOGGER.info("Received events by EventHub: {}", receivedPayloads.size());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        LOGGER
                .info(
                        "All messages processed by producer client, waiting additional 10 seconds for async consumer client to receive the events for assertions..."
                );
        Assertions.assertDoesNotThrow(() -> Thread.sleep(10000));
        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }

        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
        /*
         * Stop the deferred processing thread
         */
        deferredSyslog.run.set(false);
        try {
            deferredProcessingThread.join();
        }
        catch (InterruptedException interruptedException) {
            throw new RuntimeException(interruptedException);
        }
        consumer.close();
        relp.close();
        amqpClient.close();
    }

    private Thread sendBatch(int port, RelpBatch relpBatch) {
        Runnable runnable = () -> {
            final RelpConnection relpConnection = new RelpConnection();
            relpConnection.setWriteTimeout(10000);
            relpConnection.setConnectionTimeout(10000);
            relpConnection.setReadTimeout(10000);
            try {
                relpConnection.connect("localhost", port);
            }
            catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            }
            boolean notSent = true;
            while (notSent) {
                try {
                    relpConnection.commit(relpBatch); // send batch
                }
                catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
                if (!relpBatch.verifyTransactionAll()) { // failed batch
                    relpBatch.retryAllFailed(); // re-queue failed events
                    relpConnection.tearDown(); // teardown connection
                    try {
                        relpConnection.connect("localhost", port); // reconnect
                    }
                    catch (IOException | TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
                else { // successful batch
                    notSent = false;
                }
            }
        };
        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
    }
}
