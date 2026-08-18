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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class IntegrationDeferredTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTest.class);

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
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP amqpClient = new AMQP(connectionString, "eh1", 1, publishListener, amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, publishListener, 1024);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send 2 messages to the RELP server.
        final RelpConnection relpConnection = new RelpConnection();
        final int port = 1601;
        Assertions.assertDoesNotThrow(() -> relpConnection.connect("localhost", port));
        final RelpBatch relpBatch = new RelpBatch();
        long reqId1 = relpBatch.insert("Hello World! 1".getBytes(StandardCharsets.UTF_8));
        long reqId2 = relpBatch.insert("Hello World! 2".getBytes(StandardCharsets.UTF_8));
        Assertions.assertAll(() -> relpConnection.commit(relpBatch));
        // Wait for the AMQP scheduler to flush any remaining batches and close.
        amqpClient.close();
        while (amqpMeter.getCount() < 1) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        // verify successful transaction
        Assertions.assertTrue(relpBatch.verifyTransaction(reqId1));
        Assertions.assertTrue(relpBatch.verifyTransaction(reqId2));
        Assertions.assertAll(relpConnection::disconnect);
        relp.close();

        final String partitionId = "0";
        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from partition '0' and returns the first 100 received or until the 10 seconds has elapsed.
        final IterableStream<PartitionEvent> events = eventHubConsumerClient
                .receiveFromPartition(partitionId, 2, startingPosition, Duration.ofSeconds(10));

        final Iterator<PartitionEvent> iterator = events.iterator();
        Assertions.assertTrue(iterator.hasNext());
        PartitionEvent first = iterator.next();
        Assertions.assertEquals("Hello World! 1", first.getData().getBodyAsString());
        PartitionEvent second = iterator.next();
        Assertions.assertEquals("Hello World! 2", second.getData().getBodyAsString());
        Assertions.assertFalse(iterator.hasNext());
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
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP amqpClient = new AMQP(connectionString, "eh1", 1, publishListener, amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, publishListener, 1024);
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
        amqpClient.close();
        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }
        Assertions.assertAll(relpConnection::disconnect);
        relp.close();

        final String partitionId = "0";
        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from partition '0' and returns the first 1000 received or until the 240 seconds has elapsed.
        final IterableStream<PartitionEvent> events = eventHubConsumerClient
                .receiveFromPartition(partitionId, 1000, startingPosition, Duration.ofSeconds(240));

        final Iterator<PartitionEvent> iterator = events.iterator();
        final List<String> resultPayloads = new ArrayList<>();
        while (iterator.hasNext()) {
            PartitionEvent event = iterator.next();
            resultPayloads.add(event.getData().getBodyAsString());
        }
        Assertions.assertEquals(1000, amqpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(resultPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
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
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP amqpClient = new AMQP(connectionString, "eh1", 1, publishListener, amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, publishListener, 1024);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send 10 batches of 1000 messages to the RELP server.
        final RelpConnection relpConnection = new RelpConnection();
        final int port = 1601;
        Assertions.assertDoesNotThrow(() -> relpConnection.connect("localhost", port));
        int cursor = 1;
        final List<String> expectedPayloads = new ArrayList<>();
        for (int j = 1; j <= 10; j++) {
            final RelpBatch relpBatch = new RelpBatch();
            final List<Long> reqIds = new ArrayList<>();
            for (int i = cursor; i < cursor + 1000; i++) {
                String payload = "Hello World" + i;
                reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
                expectedPayloads.add(payload);
            }
            Assertions.assertAll(() -> relpConnection.commit(relpBatch));
            // verify successful transaction
            for (Long reqId : reqIds) {
                Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
            }
            cursor += 1000;
        }
        // Wait for the AMQP scheduler to flush events to eventhub
        amqpClient.close();
        while (amqpMeter.getCount() < 10000) {
            LOGGER.info("Waiting for events to be received... " + amqpMeter.getCount() + "/10000");
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        // verify successful transaction

        Assertions.assertAll(relpConnection::disconnect);
        relp.close();

        final String partitionId = "0";
        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from partition '0' and returns the first 10000 received or until the 240 seconds has elapsed.
        final IterableStream<PartitionEvent> events = eventHubConsumerClient
                .receiveFromPartition(partitionId, 10000, startingPosition, Duration.ofSeconds(240));

        final Iterator<PartitionEvent> iterator = events.iterator();
        final List<String> resultPayloads = new ArrayList<>();
        while (iterator.hasNext()) {
            PartitionEvent event = iterator.next();
            resultPayloads.add(event.getData().getBodyAsString());
        }
        Assertions.assertEquals(10000, amqpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(resultPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
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

        // Create consumer client to assert that producer works as expected.
        final EventHubConsumerClient eventHubConsumerClient = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildConsumerClient();

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP amqpClient = new AMQP(connectionString, "eh1", 1, publishListener, amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, publishListener, 10024);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send batch of 10000 messages to the RELP server.
        final RelpConnection relpConnection = new RelpConnection();
        final int port = 1601;
        Assertions.assertDoesNotThrow(() -> relpConnection.connect("localhost", port));
        final RelpBatch relpBatch = new RelpBatch();
        List<Long> reqIds = new ArrayList<>();
        List<String> expectedPayloads = new ArrayList<>();
        for (int i = 1; i <= 10000; i++) {
            String payload = "Hello World " + i;
            reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
            expectedPayloads.add(payload);
        }

        Assertions.assertAll(() -> relpConnection.commit(relpBatch));
        // Wait for the AMQP scheduler to flush any remaining batches and close.
        amqpClient.close();
        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }
        Assertions.assertAll(relpConnection::disconnect);
        relp.close();

        final String partitionId = "0";
        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from partition '0' and returns the first 10000 received or until the 240 seconds has elapsed.
        final IterableStream<PartitionEvent> events = eventHubConsumerClient
                .receiveFromPartition(partitionId, 10000, startingPosition, Duration.ofSeconds(240));

        final Iterator<PartitionEvent> iterator = events.iterator();
        final List<String> resultPayloads = new ArrayList<>();
        while (iterator.hasNext()) {
            PartitionEvent event = iterator.next();
            resultPayloads.add(event.getData().getBodyAsString());
        }
        Assertions.assertEquals(10000, amqpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(resultPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
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

        // Create consumer client to assert that producer works as expected.
        final EventHubConsumerClient eventHubConsumerClient = new EventHubClientBuilder()
                .connectionString(eventHubs.getConnectionString())
                .fullyQualifiedNamespace("emulatorNs1")
                .eventHubName("eh1")
                .consumerGroup("cg1")
                .buildConsumerClient();

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP amqpClient = new AMQP(connectionString, "eh1", 1, publishListener, amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", relpCommandConsumerMap);
        Thread relpThread = new Thread(relp);
        relpThread.start();
        /*
         * Start deferred processing, otherwise our client will wait forever for a response
         */
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, publishListener, 100024);
        Thread deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send batch of 100000 messages to the RELP server.
        final RelpConnection relpConnection = new RelpConnection();
        final int port = 1601;
        Assertions.assertDoesNotThrow(() -> relpConnection.connect("localhost", port));
        final RelpBatch relpBatch = new RelpBatch();
        List<Long> reqIds = new ArrayList<>();
        List<String> expectedPayloads = new ArrayList<>();
        for (int i = 1; i <= 100000; i++) {
            String payload = "Hello World " + i;
            reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
            expectedPayloads.add(payload);
        }

        Assertions.assertAll(() -> relpConnection.commit(relpBatch));
        // Wait for the AMQP scheduler to flush any remaining batches and close.
        amqpClient.close();
        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }
        Assertions.assertAll(relpConnection::disconnect);
        relp.close();

        final String partitionId = "0";
        final Instant twelveHoursAgo = Instant.now().minus(Duration.ofHours(12));
        final EventPosition startingPosition = EventPosition.fromEnqueuedTime(twelveHoursAgo);
        // Read events from partition '0' and returns the first 100000 received or until the 2400 seconds has elapsed.
        final IterableStream<PartitionEvent> events = eventHubConsumerClient
                .receiveFromPartition(partitionId, 100000, startingPosition, Duration.ofSeconds(2400));

        final Iterator<PartitionEvent> iterator = events.iterator();
        final List<String> resultPayloads = new ArrayList<>();
        while (iterator.hasNext()) {
            PartitionEvent event = iterator.next();
            resultPayloads.add(event.getData().getBodyAsString());
        }
        Assertions.assertEquals(100000, amqpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(resultPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
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
}
