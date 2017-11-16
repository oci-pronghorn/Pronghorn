package com.ociweb.pronghorn.network;

// TODO: split server from client concerns into interfaces
public interface TLSCertificates {
    String keyStoreResourceName();
    String trustStroreResourceName();
    String keyStorePassword();
    String keyPassword();

    TLSCertificates defaultCerts = new TLSCertificates() {
        @Override
        public String keyStoreResourceName() {
            return "/certificates/client.jks";
        }

        @Override
        public String trustStroreResourceName() {
            return "/certificates/trustedCerts.jks";
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
