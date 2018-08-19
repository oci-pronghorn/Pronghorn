package com.ociweb.pronghorn.network;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.io.InputStream;

public class SSLEngineFactory {
    private TLSService privateService;
	private final TLSCertificates certificates;

    SSLEngineFactory(TLSCertificates certificates) {
        this.certificates = certificates;
    }

    public SSLEngine createSSLEngine(String host, int port) {
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
            InputStream identityStoreInputStream = null;
            InputStream trustInputStream = null;
            try {
                String identityStoreResourceName = certificates.identityStoreResourceName();
                if (identityStoreResourceName != null) {
                    identityStoreInputStream = TLSCertificates.class.getResourceAsStream(identityStoreResourceName);
                    if (identityStoreInputStream == null) {
                        throw new RuntimeException(String.format("Resource %s not found", identityStoreResourceName));
                    }
                }
                String trustStroreResourceName = certificates.trustStoreResourceName();
                if (trustStroreResourceName != null) {
                    trustInputStream = TLSCertificates.class.getResourceAsStream(trustStroreResourceName);
                    if (trustInputStream == null) {
                        throw new RuntimeException(String.format("Resource %s not found", trustStroreResourceName));
                    }
                }

                privateService = TLSService.make(identityStoreInputStream, 
                		                         certificates.keyStorePassword(), 
                		                         trustInputStream, 
                		                         certificates.keyPassword(), 
                		                         certificates.trustAllCerts());
            }
            finally {
                if (identityStoreInputStream != null ) {
                    try {
                        identityStoreInputStream.close();
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
