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

import com.codahale.metrics.MetricRegistry;
import com.teragrep.aew_01.config.AmqpConfig;
import com.teragrep.aew_01.config.MetricsConfig;
import com.teragrep.aew_01.config.RelpConfig;
import com.teragrep.aew_01.config.source.Sourceable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    // Start the server
    public static void main(String[] args) {
        final MetricRegistry metricRegistry = new MetricRegistry();
        final Sourceable configSource = ConfigSource.getConfigSource();
        final RelpConfig relpConfig = new RelpConfig(configSource);
        final AmqpConfig amqpConfig = new AmqpConfig(configSource);
        final MetricsConfig metricsConfig = new MetricsConfig(configSource);
        try (final SinkServer server = new SinkServer(metricRegistry, relpConfig, amqpConfig, metricsConfig)) {
            server.start();
        }
        catch (Exception e) {
            LOGGER.error("Error starting server", e);
            throw new RuntimeException(e);
        }
    }
}
