package com.ociweb.pronghorn.network;

import java.io.InputStream;

// TODO: split server from client concerns into interfaces
// TODO: do not use input streams on public interface
public interface TLSCertificates {
    InputStream keyInputStream();
    InputStream trustInputStream();
    String keyStorePassword();
    String keyPassword();

    TLSCertificates defaultCerts = new TLSCertificates() {
        @Override
        public InputStream keyInputStream() {
            return TLSCertificates.class.getResourceAsStream("/client.jks");

        }

        @Override
        public InputStream trustInputStream() {
            return TLSCertificates.class.getResourceAsStream("/trustedCerts.jks");
        }

        @Override
        public String keyStorePassword() {
            return "storepass";
        }

        @Override
        public String keyPassword() {
            return "keypass";
        }
    };
}
