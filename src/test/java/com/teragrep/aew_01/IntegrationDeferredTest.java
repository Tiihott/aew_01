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

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.azure.EventHubsEmulatorContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        Meter relpMeter = metricRegistry.meter("relpMeter");
        final AMQP amqpClient = new AMQP(connectionString, "eh1", amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", 1, relpCommandConsumerMap);
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
        // send 2 messages to the RELP server in a single batch.
        final int port = 1601;
        int cursor = 1;
        final List<String> expectedPayloads = new ArrayList<>();
        List<Thread> sendThreads = new LinkedList<>();
        final RelpBatch relpBatch = new RelpBatch();
        final List<Long> reqIds = new ArrayList<>();
        for (int i = cursor; i < cursor + 2; i++) {
            String payload = "Hello World" + i;
            reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
            expectedPayloads.add(payload);
        }
        sendThreads.add(sendBatch(port, relpBatch));

        // Wait for the AMQP to flush all events to eventhub
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        while (amqpMeter.getCount() != 0 && amqpMeter.getCount() < 2) {
            LOGGER.info("Waiting for events to be processed by AMQP... {}/2", amqpMeter.getCount());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        while (receivedPayloads.size() != 0 && receivedPayloads.size() < 2) {
            LOGGER
                    .info(
                            "Waiting for async consumer client to receive the events for assertions... {}/2",
                            receivedPayloads.size()
                    );
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        LOGGER
                .info(
                        "All messages processed by producer client, waiting additional 30 seconds for async consumer client to receive any leftover events for assertions..."
                );
        Assertions.assertDoesNotThrow(() -> Thread.sleep(30000));
        // Close all clients and processing
        consumer.close();
        relp.close();
        amqpClient.close();
        deferredSyslog.run.set(false);
        Assertions.assertDoesNotThrow(() -> deferredProcessingThread.join());

        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }
        LOGGER.info("amqpMeter.getCount(): {}", amqpMeter.getCount());
        LOGGER.info("relpMeter.getCount(): {}", relpMeter.getCount());
        LOGGER.info("receivedPayloads.size(): {}", receivedPayloads.size());
        Assertions.assertEquals(expectedPayloads.size(), receivedPayloads.size());
        Assertions.assertEquals(2, amqpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
    }

    @Test
    void testDeferredRelpAndAmqpSingleMediumBatch() {

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

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        Meter relpMeter = metricRegistry.meter("relpMeter");
        final AMQP amqpClient = new AMQP(connectionString, "eh1", amqpMeter);

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", 1, relpCommandConsumerMap);
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
        final int port = 1601;
        int cursor = 1;
        final List<String> expectedPayloads = new ArrayList<>();
        List<Thread> sendThreads = new LinkedList<>();
        final RelpBatch relpBatch = new RelpBatch();
        final List<Long> reqIds = new ArrayList<>();
        for (int i = cursor; i < cursor + 1000; i++) {
            String payload = "Hello World" + i;
            reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
            expectedPayloads.add(payload);
        }
        sendThreads.add(sendBatch(port, relpBatch));

        // Wait for the AMQP to flush all events to eventhub
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        while (amqpMeter.getCount() != 0 && amqpMeter.getCount() < 1000) {
            LOGGER.info("Waiting for events to be processed by AMQP... {}/1000", amqpMeter.getCount());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        while (receivedPayloads.size() != 0 && receivedPayloads.size() < 1000) {
            LOGGER
                    .info(
                            "Waiting for async consumer client to receive the events for assertions... {}/1000",
                            receivedPayloads.size()
                    );
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        LOGGER
                .info(
                        "All messages processed by producer client, waiting additional 30 seconds for async consumer client to receive any leftover events for assertions..."
                );
        Assertions.assertDoesNotThrow(() -> Thread.sleep(30000));
        // Close all clients and processing
        consumer.close();
        relp.close();
        amqpClient.close();
        deferredSyslog.run.set(false);
        Assertions.assertDoesNotThrow(() -> deferredProcessingThread.join());

        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }
        // EventHub Emulator throttling will cause semdBatch() to trigger RELP read timeout that will resend messages.
        LOGGER.info("amqpMeter.getCount(): {}", amqpMeter.getCount());
        LOGGER.info("relpMeter.getCount(): {}", relpMeter.getCount());
        LOGGER.info("receivedPayloads.size(): {}", receivedPayloads.size());
        // EventHub Emulator throttling will cause additional duplicate messages to appear in EventHub even without RELP read timeouts.
        Assertions.assertTrue(expectedPayloads.size() <= receivedPayloads.size());
        Assertions.assertTrue(1000 <= amqpMeter.getCount());
        Assertions.assertTrue(1000 <= relpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
    }

    @Test
    void testDeferredRelpAndAmqpMultipleMediumBatches() {

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

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", 1, relpCommandConsumerMap);
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
        for (int j = 1; j <= 10; j++) {
            final RelpBatch relpBatch = new RelpBatch();
            final List<Long> reqIds = new ArrayList<>();
            for (int i = cursor; i < cursor + 1000; i++) {
                String payload = "Hello World" + i;
                reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
                expectedPayloads.add(payload);
            }
            sendThreads.add(sendBatch(port, relpBatch));
            cursor += 1000;
        }
        // Wait for the AMQP to flush all events to eventhub
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        while (amqpMeter.getCount() != 0 && amqpMeter.getCount() < 10000) {
            LOGGER.info("Waiting for events to be processed by AMQP... {}/10000", amqpMeter.getCount());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        while (receivedPayloads.size() != 0 && receivedPayloads.size() < 10000) {
            LOGGER
                    .info(
                            "Waiting for async consumer client to receive the events for assertions... {}/10000",
                            receivedPayloads.size()
                    );
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        LOGGER
                .info(
                        "All messages processed by producer client, waiting additional 30 seconds for async consumer client to receive any leftover events for assertions..."
                );
        Assertions.assertDoesNotThrow(() -> Thread.sleep(30000));
        // Close all clients and processing
        consumer.close();
        relp.close();
        amqpClient.close();
        deferredSyslog.run.set(false);
        Assertions.assertDoesNotThrow(() -> deferredProcessingThread.join());

        // EventHub Emulator throttling will cause semdBatch() to trigger RELP read timeout that will resend messages.
        LOGGER.info("amqpMeter.getCount(): {}", amqpMeter.getCount());
        LOGGER.info("relpMeter.getCount(): {}", relpMeter.getCount());
        LOGGER.info("receivedPayloads.size(): {}", receivedPayloads.size());
        // EventHub Emulator throttling will cause additional duplicate messages to appear in EventHub even without RELP read timeouts.
        Assertions.assertTrue(expectedPayloads.size() <= receivedPayloads.size());
        Assertions.assertTrue(10000 <= amqpMeter.getCount());
        Assertions.assertTrue(10000 <= relpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (final String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
    }

    @Test
    void testDeferredRelpAndAmqp100x100Batches() {

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

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", 1, relpCommandConsumerMap);
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
        }
        // Wait for the AMQP to flush all events to eventhub
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        while (amqpMeter.getCount() != 0 && amqpMeter.getCount() < 10000) {
            LOGGER.info("Waiting for events to be processed by AMQP... {}/10000", amqpMeter.getCount());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        while (receivedPayloads.size() != 0 && receivedPayloads.size() < 10000) {
            LOGGER
                    .info(
                            "Waiting for async consumer client to receive the events for assertions... {}/10000",
                            receivedPayloads.size()
                    );
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        LOGGER
                .info(
                        "All messages processed by producer client, waiting additional 30 seconds for async consumer client to receive any leftover events for assertions..."
                );
        Assertions.assertDoesNotThrow(() -> Thread.sleep(30000));
        // Close all clients and processing
        consumer.close();
        relp.close();
        amqpClient.close();
        deferredSyslog.run.set(false);
        Assertions.assertDoesNotThrow(() -> deferredProcessingThread.join());

        // EventHub Emulator throttling will cause semdBatch() to trigger RELP read timeout that will resend messages.
        LOGGER.info("amqpMeter.getCount(): {}", amqpMeter.getCount());
        LOGGER.info("relpMeter.getCount(): {}", relpMeter.getCount());
        LOGGER.info("receivedPayloads.size(): {}", receivedPayloads.size());
        // EventHub Emulator throttling will cause additional duplicate messages to appear in EventHub even without RELP read timeouts.
        Assertions.assertTrue(expectedPayloads.size() <= receivedPayloads.size());
        Assertions.assertTrue(10000 <= amqpMeter.getCount());
        Assertions.assertTrue(10000 <= relpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
    }

    @Test
    void testDeferredRelpAndAmqp100x1000Batches() {

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

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", 1, relpCommandConsumerMap);
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
            for (int i = cursor; i < cursor + 1000; i++) {
                String payload = "Hello World" + i;
                reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
                expectedPayloads.add(payload);
            }
            sendThreads.add(sendBatch(port, relpBatch));
            cursor += 1000;
        }
        // Wait for the AMQP to flush all events to eventhub
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        while (amqpMeter.getCount() != 0 && amqpMeter.getCount() < 100000) {
            LOGGER.info("Waiting for events to be processed by AMQP... {}/100000", amqpMeter.getCount());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        while (receivedPayloads.size() != 0 && receivedPayloads.size() < 100000) {
            LOGGER
                    .info(
                            "Waiting for async consumer client to receive the events for assertions... {}/100000",
                            receivedPayloads.size()
                    );
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        LOGGER
                .info(
                        "All messages processed by producer client, waiting additional 30 seconds for async consumer client to receive any leftover events for assertions..."
                );
        Assertions.assertDoesNotThrow(() -> Thread.sleep(30000));
        // Close all clients and processing
        consumer.close();
        relp.close();
        amqpClient.close();
        deferredSyslog.run.set(false);
        Assertions.assertDoesNotThrow(() -> deferredProcessingThread.join());

        // EventHub Emulator throttling will cause semdBatch() to trigger RELP read timeout that will resend messages.
        LOGGER.info("amqpMeter.getCount(): {}", amqpMeter.getCount());
        LOGGER.info("relpMeter.getCount(): {}", relpMeter.getCount());
        LOGGER.info("receivedPayloads.size(): {}", receivedPayloads.size());
        // EventHub Emulator throttling will cause additional duplicate messages to appear in EventHub even without RELP read timeouts..
        Assertions.assertTrue(expectedPayloads.size() <= receivedPayloads.size());
        Assertions.assertTrue(100000 <= amqpMeter.getCount());
        Assertions.assertTrue(100000 <= relpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
    }

    @Test
    void testDeferredRelpAndAmqpSingleLargeBatch() {

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

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", 1, relpCommandConsumerMap);
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

        // Wait for the AMQP to flush all events to eventhub
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        while (amqpMeter.getCount() != 0 && amqpMeter.getCount() < 10000) {
            LOGGER.info("Waiting for events to be processed by AMQP... {}/10000", amqpMeter.getCount());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        while (receivedPayloads.size() != 0 && receivedPayloads.size() < 10000) {
            LOGGER
                    .info(
                            "Waiting for async consumer client to receive the events for assertions... {}/10000",
                            receivedPayloads.size()
                    );
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        LOGGER
                .info(
                        "All messages processed by producer client, waiting additional 30 seconds for async consumer client to receive any leftover events for assertions..."
                );
        Assertions.assertDoesNotThrow(() -> Thread.sleep(30000));
        // Close all clients and processing
        consumer.close();
        relp.close();
        amqpClient.close();
        deferredSyslog.run.set(false);
        Assertions.assertDoesNotThrow(() -> deferredProcessingThread.join());
        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }

        // EventHub Emulator throttling will cause semdBatch() to trigger RELP read timeout that will resend messages.
        LOGGER.info("amqpMeter.getCount(): {}", amqpMeter.getCount());
        LOGGER.info("relpMeter.getCount(): {}", relpMeter.getCount());
        LOGGER.info("receivedPayloads.size(): {}", receivedPayloads.size());
        // EventHub Emulator throttling will cause additional duplicate messages to appear in EventHub even without RELP read timeouts..
        Assertions.assertTrue(expectedPayloads.size() <= receivedPayloads.size());
        Assertions.assertTrue(10000 <= amqpMeter.getCount());
        Assertions.assertTrue(10000 <= relpMeter.getCount());
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
    }

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

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", 1, relpCommandConsumerMap);
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

        // Wait for the AMQP to flush all events to eventhub
        LOGGER.info("relpBatch.verifyTransactionAll(): {}", relpBatch.verifyTransactionAll());
        for (Thread thread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> thread.join());
        }
        while (amqpMeter.getCount() != 0 && amqpMeter.getCount() < 100000) {
            LOGGER.info("Waiting for events to be processed by RELP... {}/100000", relpMeter.getCount());
            LOGGER.info("Waiting for events to be processed by AMQP... {}/100000", amqpMeter.getCount());
            LOGGER.info("relpBatch.verifyTransactionAll(): {}", relpBatch.verifyTransactionAll());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        while (receivedPayloads.size() != 0 && receivedPayloads.size() < 100000) {
            LOGGER
                    .info(
                            "Waiting for async consumer client to receive the events for assertions... {}/100000",
                            receivedPayloads.size()
                    );
            LOGGER.info("relpBatch.verifyTransactionAll(): {}", relpBatch.verifyTransactionAll());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        LOGGER
                .info(
                        "All messages processed by producer client, waiting additional 30 seconds for async consumer client to receive any leftover events for assertions..."
                );
        Assertions.assertDoesNotThrow(() -> Thread.sleep(30000));
        // Close all clients and processing
        consumer.close();
        relp.close();
        amqpClient.close();
        deferredSyslog.run.set(false);
        Assertions.assertDoesNotThrow(() -> deferredProcessingThread.join());

        // EventHub Emulator throttling will cause semdBatch() to trigger RELP read timeout that will resend messages.
        LOGGER.info("amqpMeter.getCount(): {}", amqpMeter.getCount());
        LOGGER.info("relpMeter.getCount(): {}", relpMeter.getCount());
        LOGGER.info("receivedPayloads.size(): {}", receivedPayloads.size());
        // EventHub Emulator throttling will cause additional duplicate messages to appear in EventHub even without RELP read timeouts..
        Assertions.assertTrue(expectedPayloads.size() <= receivedPayloads.size());
        Assertions.assertTrue(100000 <= amqpMeter.getCount());
        Assertions.assertTrue(100000 <= relpMeter.getCount());
        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }

        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
    }

    private Thread sendBatch(int port, RelpBatch relpBatch) {
        Runnable runnable = () -> {
            final RelpConnection relpConnection = new RelpConnection();
            relpConnection.setWriteTimeout(30000);
            relpConnection.setConnectionTimeout(30000);
            relpConnection.setReadTimeout(30000);
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
                    LOGGER.error("Error while committing transaction: ", e);
                    e.printStackTrace();
                }
                if (!relpBatch.verifyTransactionAll()) { // failed batch
                    relpBatch.retryAllFailed(); // re-queue failed events
                    LOGGER
                            .error(
                                    "Failed to verify RELP batch transaction, retrying sending the {} failed messages",
                                    relpBatch.getWorkQueueLength()
                            );
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
                    try {
                        relpConnection.disconnect();
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
    }
}
