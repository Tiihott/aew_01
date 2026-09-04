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
import com.teragrep.net_01.channel.buffer.writable.Writeable;
import com.teragrep.rlp_03.frame.RelpFrame;
import com.teragrep.rlp_03.frame.RelpFrameFactory;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DeferredSyslog implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeferredSyslog.class);

    private final BlockingQueue<FrameContext> frameContexts;
    private final AMQP amqpClient;
    private final PublishListener publishListener;
    private final Meter relpMeter;

    public final AtomicBoolean run;

    DeferredSyslog(
            BlockingQueue<FrameContext> frameContexts,
            AMQP amqpClient,
            PublishListener publishListener,
            int capacity,
            Meter relpMeter
    ) {
        this.frameContexts = frameContexts;
        this.amqpClient = amqpClient;
        this.publishListener = publishListener;
        this.relpMeter = relpMeter;

        this.run = new AtomicBoolean(true);
    }

    @Override
    public void run() {
        while (run.get()) {
            try {
                // this will read at least one
                FrameContext frameContext = frameContexts.poll(1, TimeUnit.SECONDS);

                if (frameContext == null) {
                    // no frame yet
                    continue;
                }

                // try-with-resources so frame is closed and freed,
                try (RelpFrame relpFrame = frameContext.relpFrame()) {
                    int establishedContextId = System.identityHashCode(frameContext.establishedContext());
                    int relpFrameId = System.identityHashCode(relpFrame);
                    final String messageId = String.valueOf(relpFrame.hashCode()); // FIXME: relpFrame.hashCode() is not unique enough. Try timestamp etc to produce unique id.
                    EventData eventData = new EventData(relpFrame.payload().toString());
                    eventData.setMessageId(messageId);
                    // Create a response for the frame, the writeable must be constructed outside the CompletableFuture.
                    RelpFrameFactory relpFrameFactory = new RelpFrameFactory();
                    RelpFrame responseFrame = relpFrameFactory.create(relpFrame.txn().toBytes(), "rsp", "200 OK");
                    Writeable writeable = responseFrame.toWriteable();
                    CompletableFuture<Boolean> acceptTransactionFuture = CompletableFuture.supplyAsync(() -> {
                        frameContext.establishedContext().egress().accept(writeable);
                        relpMeter.mark();
                        return true;
                    });
                    // Add the message to the waiting list for publishing along with the prepared response to RELP client
                    publishListener.eventWaiting(messageId, acceptTransactionFuture);
                    // Start publishing process
                    amqpClient.addEvents(eventData, publishListener);
                }
            }
            catch (Exception interruptedException) {
                LOGGER.error("Interrupted while waiting for events to complete", interruptedException);
                throw new RuntimeException(interruptedException);
                // ignored
            }
        }

    }
}
