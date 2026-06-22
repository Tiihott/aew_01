package com.teragrep.aew_01;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.teragrep.rlp_01.RelpBatch;
import com.teragrep.rlp_01.RelpConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.function.Supplier;

public class TlsRELPTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RELPTest.class);

    private static class InternalSSLContextFactory {

        public static SSLContext authenticatedContext(
                String keystorePath,
                String truststorePath,
                String keystorePassword,
                String truststorePassword,
                String protocol
        ) throws GeneralSecurityException, IOException {

            SSLContext sslContext = SSLContext.getInstance(protocol);
            KeyStore ks = KeyStore.getInstance("JKS");
            KeyStore ts = KeyStore.getInstance("JKS");

            File ksFile = new File(keystorePath);
            File tsFile = new File(truststorePath);

            try (FileInputStream ksFileIS = new FileInputStream(ksFile)) {
                try (FileInputStream tsFileIS = new FileInputStream(tsFile)) {
                    ts.load(tsFileIS, truststorePassword.toCharArray());
                    TrustManagerFactory tmf = TrustManagerFactory
                            .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(ts);

                    ks.load(ksFileIS, keystorePassword.toCharArray());
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(ks, keystorePassword.toCharArray());
                    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

                    return sslContext;
                }
            }
        }
    }

    @Test
    void testRun() {
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter relpMeter = metricRegistry.meter("relpMeter");

        final RELP relp = new RELP("true", "2601", "src/test/resources/keystore-server.jks", "changeit", frameContext -> {
            LOGGER.info(frameContext.relpFrame().payload().toString());
            relpMeter.mark();
        });
        Thread relpThread = new Thread(relp);
        relpThread.start();
        // Wait for the server to start
        Assertions.assertDoesNotThrow(() -> Thread.sleep(5 * 1000));

        // send message to the RELP server.
        SSLContext sslContext = Assertions.assertDoesNotThrow(() -> InternalSSLContextFactory
                .authenticatedContext(
                        "src/test/resources/keystore-client.jks", "src/test/resources/truststore.jks",
                        "changeit", "changeit", "TLSv1.3"
                ));
        Supplier<SSLEngine> sslEngineSupplier = sslContext::createSSLEngine;
        final RelpConnection relpConnection = new RelpConnection(sslEngineSupplier);
        final int port = 2601;
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

}
