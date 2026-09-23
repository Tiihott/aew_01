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

import com.teragrep.rlp_01.RelpCommand;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.delegate.event.RelpEvent;
import com.teragrep.rlp_03.frame.delegate.event.RelpEventClose;
import com.teragrep.rlp_03.frame.delegate.event.RelpEventOpen;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class RelpCommandConsumerMapBuilder {

    private final Map<String, RelpEvent> relpCommandConsumerMap;
    private final BlockingQueue<FrameContext> frameContexts;

    RelpCommandConsumerMapBuilder(
            HashMap<String, RelpEvent> relpCommandConsumerMap,
            BlockingQueue<FrameContext> frameContexts
    ) {
        this.relpCommandConsumerMap = relpCommandConsumerMap;
        this.frameContexts = frameContexts;
    }

    RelpCommandConsumerMapBuilder(int frameContextsCapacity) {
        this(new HashMap<>(), new ArrayBlockingQueue<>(frameContextsCapacity));
    };

    RelpCommandConsumerMapBuilder() {
        this(new HashMap<>(), new ArrayBlockingQueue<>(1024));
    }

    public void buildRelpCommandConsumerMap() {
        if (!relpCommandConsumerMap.isEmpty()) {
            throw new IllegalStateException("RelpCommandConsumerMap is already built");
        }
        relpCommandConsumerMap.put(RelpCommand.OPEN, new RelpEventOpen());
        relpCommandConsumerMap.put(RelpCommand.CLOSE, new RelpEventClose());
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
    }

    public Map<String, RelpEvent> relpCommandConsumerMap() {
        return relpCommandConsumerMap;
    }

    public BlockingQueue<FrameContext> frameContexts() {
        return frameContexts;
    }
}
