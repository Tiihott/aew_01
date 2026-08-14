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

import com.teragrep.net_01.channel.socket.PlainFactory;
import com.teragrep.net_01.channel.socket.SocketFactory;
import com.teragrep.net_01.channel.socket.TLSFactory;
import com.teragrep.net_01.eventloop.EventLoop;
import com.teragrep.net_01.eventloop.EventLoopFactory;
import com.teragrep.net_01.server.ServerFactory;
import com.teragrep.rlp_03.frame.FrameDelegationClockFactory;
import com.teragrep.rlp_03.frame.delegate.DefaultFrameDelegate;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.delegate.FrameDelegate;
import com.teragrep.rlp_03.frame.delegate.event.RelpEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class RELP implements Runnable, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RELP.class);

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final EventLoop eventLoop;
    private final Thread eventLoopThread;

    // The syslogConsumer will be responsible for passing on the relp payloads to the AMQP. Currently, logger is used.
    private final Supplier<FrameDelegate> frameDelegateSupplier;
    private final EventLoopFactory eventLoopFactory = new EventLoopFactory();

    final String tls;
    final String port;
    final String tlsKeystore;
    final String tlsKeystorePassword;

    public RELP(
            final String tls,
            final String port,
            final String tlsKeystore,
            final String tlsKeystorePassword,
            Consumer<FrameContext> syslogConsumer
    ) {
        this.tls = tls;
        this.port = port;
        this.tlsKeystore = tlsKeystore;
        this.tlsKeystorePassword = tlsKeystorePassword;
        this.frameDelegateSupplier = () -> {
            LOGGER.debug("Providing frameDelegate for a connection");
            return new DefaultFrameDelegate(syslogConsumer);
        };
        try {
            eventLoop = eventLoopFactory.create();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        eventLoopThread = new Thread(eventLoop);
    }

    public RELP(
            final String tls,
            final String port,
            final String tlsKeystore,
            final String tlsKeystorePassword,
            Map<String, RelpEvent> relpCommandConsumerMap
    ) {
        this.tls = tls;
        this.port = port;
        this.tlsKeystore = tlsKeystore;
        this.tlsKeystorePassword = tlsKeystorePassword;
        this.frameDelegateSupplier = () -> {
            LOGGER.debug("Providing frameDelegate for a connection");
            return new DefaultFrameDelegate(relpCommandConsumerMap);
        };
        try {
            eventLoop = eventLoopFactory.create();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        eventLoopThread = new Thread(eventLoop);
    }

    @Override
    public void run() {

        eventLoopThread.start();

        final SocketFactory socketFactory;

        if (Boolean.parseBoolean(tls)) {
            socketFactory = tlsServer();
        }
        else {
            LOGGER.debug("Starting plain server on port <[{}]>", port);
            socketFactory = new PlainFactory();
        }

        final ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                socketFactory,
                new FrameDelegationClockFactory(frameDelegateSupplier)
        );

        try {
            serverFactory.create(Integer.parseInt(port));
        }
        catch (IOException e) {
            LOGGER.error("Failed to run: <[{}]>", e.getMessage(), e);
            throw new UncheckedIOException(e);
        }

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.debug("Stopping server at port <[{}]>", port);

            latch.countDown();
        }));

        while (true)
            try {
                latch.await();
                break;
            }
            catch (InterruptedException e) {
                LOGGER.debug("Interruption in main thread latch.await(), retrying", e);
            }
    }

    private TLSFactory tlsServer() {
        LOGGER.debug("Starting TLS server on port <[{}]>", port);

        final InputStream keystoreStream;
        if (tlsKeystore != null) {
            LOGGER.debug("Using user supplied keystore");
            Path path = Paths.get(tlsKeystore);
            if (!path.toFile().exists()) {
                throw new RuntimeException("File " + tlsKeystore + " doesn't exist");
            }
            try {
                keystoreStream = Files.newInputStream(path);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        else {
            LOGGER.debug("Using default keystore");
            // get server keyStore as inputstream, works on JAR packaging as well this way
            keystoreStream = Main.class.getClassLoader().getResourceAsStream("keystore-server.jks");
        }

        SSLContext sslContext;
        try {
            sslContext = TLSContextFactory.authenticatedContext(keystoreStream, tlsKeystorePassword, "TLSv1.3");
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException("Can't create sslContext: " + e);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Function<SSLContext, SSLEngine> sslEngineFunction = sslCtx -> {
            SSLEngine sslEngine = sslCtx.createSSLEngine();
            sslEngine.setUseClientMode(false);
            return sslEngine;
        };

        return new TLSFactory(sslContext, sslEngineFunction);
    }

    @Override
    public void close() {
        eventLoop.stop();
        try {
            eventLoopThread.join();
        }
        catch (InterruptedException interruptedException) {
            throw new RuntimeException(interruptedException);
        }
        LOGGER.debug("Server stopped at port <[{}]>", port);
        executorService.shutdown();
    }
}
