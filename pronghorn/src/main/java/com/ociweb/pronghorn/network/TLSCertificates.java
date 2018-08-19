package com.ociweb.pronghorn.network;

// TODO: split server from client concerns into interfaces
public interface TLSCertificates {
    String identityStoreResourceName();
    String trustStoreResourceName();
    String keyStorePassword();
    String keyPassword();
    // if true ignores trustStoreResourceName and uses a trust all TrustManager
    boolean trustAllCerts();
    boolean clientAuthRequired();
    
    class Default implements TLSCertificates {
        @Override
        public String identityStoreResourceName() {
        	// to gen:  keytool -genkey -keyalg RSA -alias tomcat -keystore selfsigned.jks -validity <days> -keysize 2048
        	// these are the certs you are trying to 'be'
        	// keytool -list -keystore client.jks
            return "/certificates/client.jks";
        }

        @Override
        public String trustStoreResourceName() {
        	// these are the certs you trust
        	// keytool -list -keystore trustedCerts.jks
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

        @Override
        public boolean trustAllCerts() {
            return true;
        }
        @Override
        public boolean clientAuthRequired() {
    		return false;
    	}
    }

    TLSCertificates defaultCerts = new Default();
	    
}
