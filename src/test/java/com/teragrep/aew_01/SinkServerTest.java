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
import com.codahale.metrics.MetricRegistry;
import com.teragrep.aew_01.config.AmqpConfig;
import com.teragrep.aew_01.config.MetricsConfig;
import com.teragrep.aew_01.config.RelpConfig;
import com.teragrep.rlp_01.RelpBatch;
import com.teragrep.rlp_01.RelpConnection;
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

class SinkServerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SinkServerTest.class);

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
        LOGGER.info("eventHubs.getExposedPorts(): {}", eventHubs.getExposedPorts());
        LOGGER.info("azurite.getExposedPorts(): {}", azurite.getExposedPorts());
        azurite.getExposedPorts();
        eventHubs.start();
        LOGGER.info("eventHubs.getFirstMappedPort(): {}", eventHubs.getFirstMappedPort());
    }

    @AfterEach
    void tearDown() {
        Assertions.assertDoesNotThrow(eventHubs::stop);
        Assertions.assertDoesNotThrow(azurite::stop);
        Assertions.assertDoesNotThrow(network::close);
    }

    @Test
    void testStart() {
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

        final MetricRegistry metricRegistry = new MetricRegistry();
        final String connectionString = eventHubs.getConnectionString();
        LOGGER.info("Connecting to AWS EventHubs at {}", connectionString);
        RelpConfig relpConfig = new RelpConfig("1601", "false", "changeit", "changeit", "1", "10024");
        AmqpConfig amqpConfig = new AmqpConfig(
                "eh1",
                "emulatorNs1",
                eventHubs.getConnectionString(),
                "false",
                "connectionString"
        );
        MetricsConfig metricsConfig = new MetricsConfig(9090);
        SinkServer sinkServer = new SinkServer(metricRegistry, relpConfig, amqpConfig, metricsConfig);
        Thread thread = startServer(sinkServer);
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));

        final RelpBatch relpBatch = new RelpBatch();
        List<Long> reqIds = new ArrayList<>();
        List<String> expectedPayloads = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            String payload = "Hello World " + i;
            reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
            expectedPayloads.add(payload);
        }
        List<Thread> sendThreads = new LinkedList<>();
        sendThreads.add(sendBatch(1601, relpBatch));
        // Wait for all sendBatch operations to finish
        LOGGER.info("relpBatch.verifyTransactionAll(): {}", relpBatch.verifyTransactionAll());
        for (Thread sendThread : sendThreads) {
            Assertions.assertDoesNotThrow(() -> sendThread.join());
        }
        // Wait for async consumer to receive events from event hub.
        while (receivedPayloads.size() < 1000) {
            LOGGER
                    .info(
                            "Waiting for async consumer client to receive the events for assertions... {}/1000",
                            receivedPayloads.size()
                    );
            LOGGER.info("relpBatch.verifyTransactionAll(): {}", relpBatch.verifyTransactionAll());
            Assertions.assertDoesNotThrow(() -> Thread.sleep(1000));
        }
        consumer.close();
        // Assert that all the expected payloads are present in eventhub results
        for (String expectedPayload : expectedPayloads) {
            Assertions
                    .assertTrue(receivedPayloads.contains(expectedPayload), "Message was not received by Eventhub: " + expectedPayload);
        }
        LOGGER.info("Closing the sink server");
        Assertions.assertDoesNotThrow(sinkServer::close);
        Assertions.assertDoesNotThrow(() -> thread.join());
    }

    private Thread startServer(SinkServer sinkserver) {
        Runnable runnable = sinkserver::start;
        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
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
