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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.teragrep.rlp_01.RelpBatch;
import com.teragrep.rlp_01.RelpConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class RELPTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RELPTest.class);

    @Test
    void testRun() {
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter relpMeter = metricRegistry.meter("relpMeter");

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", frameContext -> {
            LOGGER.info(frameContext.relpFrame().payload().toString());
            relpMeter.mark();
        });
        Thread relpThread = new Thread(relp);
        relpThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send message to the RELP server.
        final RelpConnection relpConnection = new RelpConnection();
        final int port = 1601;
        Assertions.assertDoesNotThrow(() -> relpConnection.connect("localhost", port));
        final RelpBatch relpBatch = new RelpBatch();
        long reqId = relpBatch.insert("Hello World!".getBytes(StandardCharsets.UTF_8));
        Assertions.assertAll(() -> relpConnection.commit(relpBatch));
        // verify successful transaction
        Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        Assertions.assertAll(relpConnection::disconnect);
        Assertions.assertEquals(1, relpMeter.getCount());
        relp.close();
    }

    @Test
    void testRunLargeBatch() {
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter relpMeter = metricRegistry.meter("relpMeter");

        final RELP relp = new RELP("false", "1601", "changeit", "changeit", frameContext -> {
            LOGGER.info(frameContext.relpFrame().payload().toString());
            relpMeter.mark();
        });
        Thread relpThread = new Thread(relp);
        relpThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));
        // send message to the RELP server.
        final RelpConnection relpConnection = new RelpConnection();
        final int port = 1601;
        Assertions.assertDoesNotThrow(() -> relpConnection.connect("localhost", port));
        final RelpBatch relpBatch = new RelpBatch();
        List<Long> reqIds = new ArrayList<>();
        for (int i = 1; i <= 10000; i++) {
            String payload = "Hello World " + i;
            reqIds.add(relpBatch.insert(payload.getBytes(StandardCharsets.UTF_8)));
        }
        Assertions.assertAll(() -> relpConnection.commit(relpBatch));
        // verify successful transaction
        for (Long reqId : reqIds) {
            Assertions.assertTrue(relpBatch.verifyTransaction(reqId));
        }
        Assertions.assertAll(relpConnection::disconnect);
        Assertions.assertEquals(10000, relpMeter.getCount());
        relp.close();
    }
}
