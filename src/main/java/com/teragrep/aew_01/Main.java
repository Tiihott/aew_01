/*
 * RELP bridge for Microsoft Azure EventHub (aew_01)
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

import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.teragrep.aew_01.config.AmqpConfig;
import com.teragrep.aew_01.config.MetricsConfig;
import com.teragrep.aew_01.config.RelpConfig;
import com.teragrep.aew_01.config.TlsConfig;
import com.teragrep.aew_01.config.source.Sourceable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    // Start the server
    public static void main(String[] args) {
        // Load configurations
        final Sourceable configSource = ConfigSource.getConfigSource();
        final RelpConfig relpConfig = new RelpConfig(configSource);
        final TlsConfig tlsConfig = new TlsConfig(configSource);
        final AmqpConfig amqpConfig = new AmqpConfig(configSource);
        final MetricsConfig metricsConfig = new MetricsConfig(configSource);

        // Initialize metrics and prometheus
        final MetricRegistry metricRegistry = new MetricRegistry();
        final Meter amqpMeter = metricRegistry.meter("amqpMeter");
        final Meter relpMeter = metricRegistry.meter("relpMeter");
        final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        final Slf4jReporter slf4jReporter = Slf4jReporter
                .forRegistry(metricRegistry)
                .outputTo(LoggerFactory.getLogger(RelpServer.class))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        final int prometheusPort = metricsConfig.prometheusPort();
        final org.eclipse.jetty.server.Server jettyServer = new org.eclipse.jetty.server.Server(prometheusPort);
        try {
            final Metrics metrics = new Metrics(jmxReporter, slf4jReporter, metricRegistry, jettyServer);
            metrics.startMetrics();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Initialize AmqpClient
        final AmqpClient amqpClient;
        if (Objects.equals(amqpConfig.connectionType(), "connectionString")) {
            amqpClient = new AmqpClient(amqpConfig.connectionString(), amqpConfig.eventHubName(), amqpMeter);
            LOGGER.info("amqpClient initialized using connection string");
        }
        else if (Objects.equals(amqpConfig.connectionType(), "passwordless")) {
            amqpClient = new AmqpClient(
                    new ManagedIdentityCredentialBuilder().clientId(amqpConfig.userManagedIdentityClientId()).build(),
                    amqpConfig.eventHubName(),
                    amqpConfig.namespaceName(),
                    amqpMeter
            );
            LOGGER.info("amqpClient initialized using passwordless connection");
        }
        else {
            throw new IllegalArgumentException("Unsupported connection type");
        }

        // Initialize RELP server that sends received messages to EventHub via AmqpClient.
        final SyslogProcessing syslogProcessing = new SyslogProcessing(amqpClient, relpMeter);
        final RelpCommandConsumers relpCommandConsumers = new RelpCommandConsumers(syslogProcessing);
        final String tls = tlsConfig.tls();
        // Initialize and run RelpServer with tls
        if (Boolean.parseBoolean(tls)) {
            try (
                    final RelpServer relpServer = new RelpServer(
                            relpConfig.port(),
                            relpCommandConsumers.asMap(),
                            tlsConfig.tlsFactory()
                    )
            ) {
                relpServer.run();
            }
        }
        // Initialize and run RelpServer without tls
        else {
            try (final RelpServer relpServer = new RelpServer(relpConfig.port(), relpCommandConsumers.asMap())) {
                relpServer.run();
            }
        }
    }
}
