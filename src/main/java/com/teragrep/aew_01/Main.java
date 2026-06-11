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

import com.azure.messaging.eventhubs.EventData;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.teragrep.aew_01.config.AmqpConfig;
import com.teragrep.aew_01.config.RelpConfig;
import com.teragrep.aew_01.config.source.EnvironmentSource;
import com.teragrep.aew_01.config.source.Sourceable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    // Start the server
    public static void main(String[] args) {
        final MetricRegistry metricRegistry = new MetricRegistry();
        final Sourceable configSource = getConfigSource();
        Meter relpMeter = metricRegistry.meter("relpMeter");
        Meter amqpMeter = metricRegistry.meter("amqpMeter");
        // load configs etc. and initialize AMQP and RELP
        final AMQP amqpClient = new AMQP(
                new AmqpConfig(configSource).connectionStringWithEventHub(),
                new AmqpConfig(configSource).eventHubName(),
                new AmqpConfig(configSource).namespaceName(),
                amqpMeter
        );
        try (
                RELP relp = new RELP(new RelpConfig(configSource).tls(), new RelpConfig(configSource).port(), new RelpConfig(configSource).tlsTruststorePassword(), new RelpConfig(configSource).tlsKeystorePassword(), frameContext -> {
                    amqpClient.publishEvents(List.of(new EventData(frameContext.relpFrame().payload().toString())));
                    relpMeter.mark();
                })
        ) {
            relp.run();
        }
        amqpClient.close();
    }

    private static Sourceable getConfigSource() {
        LOGGER.info("Getting config source...");
        final String type = System.getProperty("config.source", "properties");

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
