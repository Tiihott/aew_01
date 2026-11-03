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
package com.teragrep.aew_01.config;

import com.teragrep.aew_01.Main;
import com.teragrep.aew_01.TLSContextFactory;
import com.teragrep.aew_01.config.source.Sourceable;
import com.teragrep.net_01.channel.socket.TLSFactory;
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
import java.util.function.Function;

public final class TlsConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsConfig.class);

    private final String tls;
    private final String tlsKeystorePath;
    private final String tlsKeystorePassword;

    public TlsConfig(final Sourceable configSource) {
        this(
                configSource.source("tls.enabled"),
                configSource.source("tls.keystorePath"),
                configSource.source("tls.keystorePassword")
        );
    }

    public TlsConfig(final String tls, final String tlsKeystorePath, final String tlsKeystorePassword) {
        this.tls = tls;
        this.tlsKeystorePath = tlsKeystorePath;
        this.tlsKeystorePassword = tlsKeystorePassword;
    }

    public String tls() {
        return tls;
    }

    public String tlsKeystorePath() {
        return tlsKeystorePath;
    }

    public String tlsKeystorePassword() {
        return tlsKeystorePassword;
    }

    public TLSFactory tlsFactory() {
        if (!Boolean.parseBoolean(tls)) {
            throw new IllegalStateException("TLS is not enabled");
        }
        LOGGER.debug("Building TLSFactory");
        final InputStream keystoreStream;
        if (tlsKeystorePath != null) {
            LOGGER.debug("Using user supplied keystore");
            Path path = Paths.get(tlsKeystorePath);
            if (!path.toFile().exists()) {
                throw new RuntimeException("File " + tlsKeystorePath + " doesn't exist");
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
            sslContext = new TLSContextFactory().authenticatedContext(keystoreStream, tlsKeystorePassword, "TLSv1.3");
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
}
