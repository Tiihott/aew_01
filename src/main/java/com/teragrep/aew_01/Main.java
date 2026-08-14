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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.teragrep.aew_01.config.AmqpConfig;
import com.teragrep.aew_01.config.MetricsConfig;
import com.teragrep.aew_01.config.RelpConfig;
import com.teragrep.aew_01.config.source.EnvironmentSource;
import com.teragrep.aew_01.config.source.Sourceable;
import io.prometheus.metrics.exporter.servlet.jakarta.PrometheusMetricsServlet;
import io.prometheus.metrics.instrumentation.dropwizard.DropwizardExports;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    // Start the server
    public static void main(String[] args) {
        final MetricRegistry metricRegistry = new MetricRegistry();
        final Sourceable configSource = getConfigSource();
        Meter relpMeter = metricRegistry.meter("relpMeter");
        Meter amqpMeter = metricRegistry.meter("amqpMeter");

        final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        final Slf4jReporter slf4jReporter = Slf4jReporter
                .forRegistry(metricRegistry)
                .outputTo(LoggerFactory.getLogger(RELP.class))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        final int prometheusPort = new MetricsConfig(configSource).prometheusPort();
        final Server jettyServer = new Server(prometheusPort);
        try {
            startMetrics(jmxReporter, slf4jReporter, metricRegistry, jettyServer);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        // load configs etc. and initialize AMQP and RELP
        // create credentials using the ManagedIdentityCredentialBuilder
        LOGGER.info("Building EventHub credentials...");
        final TokenCredential credential = new ManagedIdentityCredentialBuilder()
                .clientId(new AmqpConfig(configSource).userManagedIdentityClientId())
                .build();
        LOGGER.debug("EventHub credentials built successfully");

        final PublishListener publishListener = new PublishListenerImpl();
        final AMQP amqpClient = new AMQP(
                credential,
                new AmqpConfig(configSource).eventHubName(),
                new AmqpConfig(configSource).namespaceName(),
                new AmqpConfig(configSource).maxBatchTimeS(),
                publishListener,
                amqpMeter
        );
        try (
                RELP relp = new RELP(new RelpConfig(configSource).tls(), new RelpConfig(configSource).port(), new RelpConfig(configSource).tlsTruststorePassword(), new RelpConfig(configSource).tlsKeystorePassword(), frameContext -> {
                    BufferListener bufferListener = new BufferListenerImpl();
                    amqpClient.addEvents(new EventData(frameContext.relpFrame().payload().toString()), bufferListener);
                    while (!bufferListener.complete()) {
                        try {
                            Thread.sleep(100);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (!bufferListener.result()) {
                        throw new RuntimeException("Failed to transfer events to EventHub");
                    }
                    relpMeter.mark();
                })
        ) {
            relp.run();
        }
        amqpClient.close();
    }

    private static void startMetrics(
            JmxReporter jmxReporter,
            Slf4jReporter slf4jReporter,
            MetricRegistry metricRegistry,
            Server jettyServer
    ) throws Exception {
        LOGGER.info("Starting metrics for RELP sink for Microsoft Azure EventHub...");
        jmxReporter.start();
        slf4jReporter.start(1, TimeUnit.MINUTES);

        // prometheus-exporter
        PrometheusRegistry.defaultRegistry.register(new DropwizardExports(metricRegistry));

        final ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        jettyServer.setHandler(context);

        final PrometheusMetricsServlet metricsServlet = new PrometheusMetricsServlet();
        final ServletHolder servletHolder = new ServletHolder(metricsServlet);
        context.addServlet(servletHolder, "/metrics");

        jettyServer.start();
        LOGGER.info("Metrics started for RELP sink for Microsoft Azure EventHub.");
    }

    private static Sourceable getConfigSource() {
        LOGGER.info("Getting config source...");
        final String type = System.getProperty("config.source", "environment");

        final Sourceable rv;
        if ("environment".equals(type)) {
            LOGGER.info("Config source set to environment.");
            rv = new EnvironmentSource();
        }
        else {
            throw new IllegalArgumentException("config.source not within supported types: [environment]");
        }

        return rv;
    }
}
