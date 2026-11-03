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

import com.teragrep.aew_01.config.source.EnvironmentSource;
import com.teragrep.aew_01.config.source.Sourceable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;

class TlsConfigTest {

    @Test
    void tls() {
        final TlsConfig tlsConfig = new TlsConfig("true", "keystorePathExample", "keystorePasswordExample");
        Assertions.assertEquals("true", tlsConfig.tls());
    }

    @Test
    void tlsKeystorePath() {
        final TlsConfig tlsConfig = new TlsConfig("false", "keystorePathExample", "keystorePasswordExample");
        Assertions.assertEquals("keystorePathExample", tlsConfig.tlsKeystorePath());
    }

    @Test
    void tlsKeystorePassword() {
        final TlsConfig tlsConfig = new TlsConfig("false", "keystorePathExample", "keystorePasswordExample");
        Assertions.assertEquals("keystorePasswordExample", tlsConfig.tlsKeystorePassword());
    }

    @Test
    void missingEnvironmentVariablesException() {
        final String type = System.getProperty("config.source", "environment");
        final Sourceable configSource;
        Assertions.assertEquals("environment", type);
        configSource = new EnvironmentSource();
        Assertions.assertThrows(IllegalStateException.class, () -> new TlsConfig(configSource));
    }

    @Test
    void tlsFactory() {
        final TlsConfig tlsConfig = new TlsConfig("true", "src/test/resources/keystore-server.jks", "changeit");
        Assertions.assertDoesNotThrow(tlsConfig::tlsFactory);
    }

    @Test
    void tlsFactoryDisabledException() {
        final TlsConfig tlsConfig = new TlsConfig("false", "src/test/resources/keystore-server.jks", "changeit");
        Assertions.assertThrows(IllegalStateException.class, tlsConfig::tlsFactory);
    }

    @Test
    void tlsFactoryPathException() {
        final TlsConfig tlsConfig = new TlsConfig("true", "invalidKeystorePath", "changeit");
        Assertions.assertThrows(RuntimeException.class, tlsConfig::tlsFactory);
    }

    @Test
    void tlsFactoryPasswordException() {
        final TlsConfig tlsConfig = new TlsConfig(
                "true",
                "src/test/resources/keystore-server.jks",
                "invalidKeystorePassword"
        );
        Assertions.assertThrows(UncheckedIOException.class, tlsConfig::tlsFactory);
    }
}
