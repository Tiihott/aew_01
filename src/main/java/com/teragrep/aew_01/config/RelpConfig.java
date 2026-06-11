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
package com.teragrep.aew_01.config;

import com.teragrep.aew_01.config.source.Sourceable;

public final class RelpConfig {

    private final String port;
    private final String tls;
    private final String tlsKeystorePassword;
    private final String tlsTruststorePassword;

    public RelpConfig(final Sourceable configSource) {
        this(
                configSource.source("relp.port", "<RELP PORT>"),
                configSource.source("relp.tls", "<RELP TLS>"),
                configSource.source("relp.tlsKeystorePassword", "<RELP TLS KEYSTORE PASSWORD>"),
                configSource.source("relp.tlsTruststorePassword", "<RELP TLS TRUSTSTORE PASSWORD>")
        );
    }

    public RelpConfig(String port, String tls, String tlsKeystorePassword, String tlsTruststorePassword) {
        this.port = port;
        this.tls = tls;
        this.tlsKeystorePassword = tlsKeystorePassword;
        this.tlsTruststorePassword = tlsTruststorePassword;
    }

    public String port() {
        return port;
    }

    public String tls() {
        return tls;
    }

    public String tlsKeystorePassword() {
        return tlsKeystorePassword;
    }

    public String tlsTruststorePassword() {
        return tlsTruststorePassword;
    }

}
