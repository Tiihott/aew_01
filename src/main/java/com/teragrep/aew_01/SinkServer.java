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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.teragrep.aew_01.config.AmqpConfig;
import com.teragrep.aew_01.config.MetricsConfig;
import com.teragrep.aew_01.config.RelpConfig;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.delegate.event.RelpEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public final class SinkServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SinkServer.class);

    private final MetricRegistry metricRegistry;
    private final Meter relpMeter;
    private final Meter amqpMeter;
    private final AmqpConfig amqpConfig;
    private final RelpConfig relpConfig;
    private final MetricsConfig metricsConfig;

    public SinkServer(
            MetricRegistry metricRegistry,
            RelpConfig relpConfig,
            AmqpConfig amqpConfig,
            MetricsConfig metricsConfig
    ) {
        this.metricRegistry = metricRegistry;
        this.relpMeter = metricRegistry.meter("relpMeter");
        this.amqpMeter = metricRegistry.meter("amqpMeter");
        this.amqpConfig = amqpConfig;
        this.relpConfig = relpConfig;
        this.metricsConfig = metricsConfig;
    }

    public void start() {
        final RelpCommandConsumerMapBuilder relpCommandConsumerMapBuilder = new RelpCommandConsumerMapBuilder(
                relpConfig.frameContextsCapacity()
        );
        relpCommandConsumerMapBuilder.buildRelpCommandConsumerMap();
        final Map<String, RelpEvent> relpCommandConsumerMap = relpCommandConsumerMapBuilder.relpCommandConsumerMap();
        final BlockingQueue<FrameContext> frameContexts = relpCommandConsumerMapBuilder.frameContexts();

        final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        final Slf4jReporter slf4jReporter = Slf4jReporter
                .forRegistry(metricRegistry)
                .outputTo(LoggerFactory.getLogger(RELP.class))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        final int prometheusPort = metricsConfig.prometheusPort();
        final org.eclipse.jetty.server.Server jettyServer = new org.eclipse.jetty.server.Server(prometheusPort);
        try {
            Metrics.startMetrics(jmxReporter, slf4jReporter, metricRegistry, jettyServer);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        // load configs etc. and initialize AMQP and RELP
        // create credentials using the ManagedIdentityCredentialBuilder
        LOGGER.info("Building EventHub credentials...");
        final TokenCredential credential = new ManagedIdentityCredentialBuilder()
                .clientId(amqpConfig.userManagedIdentityClientId())
                .build();
        LOGGER.debug("EventHub credentials built successfully");

        final AMQP amqpClient = new AMQP(credential, amqpConfig.eventHubName(), amqpConfig.namespaceName(), amqpMeter);
        Thread deferredProcessingThread;
        DeferredSyslog deferredSyslog = new DeferredSyslog(frameContexts, amqpClient, relpMeter);
        /*
         * Start deferred processing before running the RELP server, otherwise our client will wait forever for a response
         */
        deferredProcessingThread = new Thread(deferredSyslog);
        deferredProcessingThread.start();
        try (
                RELP relp = new RELP(
                        relpConfig.tls(),
                        relpConfig.port(),
                        relpConfig.tlsTruststorePassword(),
                        relpConfig.tlsKeystorePassword(),
                        relpConfig.processingThreads(),
                        relpCommandConsumerMap
                );
        ) {
            relp.run();
        }
        amqpClient.close();
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
