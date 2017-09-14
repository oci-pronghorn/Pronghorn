package com.ociweb.pronghorn.network;

import java.io.InputStream;

public interface TLSPolicy {
    InputStream keyInputStream();
    InputStream trustInputStream();
    String keyStorePassword();
    String keyPassword();

    static TLSPolicy defaultPolicy = new TLSPolicy() {
        @Override
        public InputStream keyInputStream() {
            return TLSPolicy.class.getResourceAsStream("/client.jks");
        }

        @Override
        public InputStream trustInputStream() {
            return SSLEngineFactory.class.getResourceAsStream("/trustedCerts.jks");
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
