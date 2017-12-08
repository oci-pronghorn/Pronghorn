package com.ociweb.pronghorn.network;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.io.InputStream;

class SSLEngineFactory {
    private TLSService privateService;
	private final TLSCertificates certificates;

    SSLEngineFactory(TLSCertificates certificates) {
        this.certificates = certificates;
    }

    SSLEngine createSSLEngine(String host, int port) {
        return getService().createSSLEngineClient(host, port);
    }

    SSLEngine createSSLEngine() {
        return getService().createSSLEngineServer();
    }

    int maxEncryptedContentLength() {
        return getService().maxEncryptedContentLength();
    }

    public void initTLSService() {
    	//ensure that we have a trust manager in place BEFORE we start accepting connections
    	getService();
    }
    
    private TLSService getService() {
        if (privateService==null) {
            InputStream keyInputStream = null;
            InputStream trustInputStream = null;
            try {
                String keyStoreResourceName = certificates.keyStoreResourceName();
                if (keyStoreResourceName != null) {
                    keyInputStream = TLSCertificates.class.getResourceAsStream(keyStoreResourceName);
                    if (keyInputStream == null) {
                        throw new RuntimeException(String.format("Resource %s not found", keyStoreResourceName));
                    }
                }
                String trustStroreResourceName = certificates.trustStroreResourceName();
                if (trustStroreResourceName != null) {
                    trustInputStream = TLSCertificates.class.getResourceAsStream(trustStroreResourceName);
                    if (trustInputStream == null) {
                        throw new RuntimeException(String.format("Resource %s not found", trustStroreResourceName));
                    }
                }

                String keyPassword = certificates.keyPassword();
                String keyStorePassword = certificates.keyStorePassword();

                privateService = TLSService.make(keyInputStream, keyStorePassword, 
                		                         trustInputStream, keyPassword, 
                		                         certificates.trustAllCerts());
            }
            finally {
                if (keyInputStream != null ) {
                    try {
                        keyInputStream.close();
                    } catch (IOException ignored) {
                        // This is not a failure for the running system
                    }
                }
                if (trustInputStream != null ) {
                    try {
                        trustInputStream.close();
                    } catch (IOException ignored) {
                        // This is not a failure for the running system
                    }
                }
            }
        }
        return privateService;
    }
}
