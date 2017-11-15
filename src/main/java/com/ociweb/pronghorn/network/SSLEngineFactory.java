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

    private TLSService getService() {
        if (privateService==null) {
            InputStream keyInputStream = null;
            InputStream trustInputStream = null;
            try {
                // Server Identity
                keyInputStream = certificates.keyInputStream();
                // All the internet sites client trusts
                trustInputStream = certificates.trustInputStream();

                String keyPassword = certificates.keyPassword();
                String keyStorePassword = certificates.keyStorePassword();

                privateService = TLSService.make(keyInputStream, keyStorePassword, trustInputStream, keyPassword, true);
            }
            finally {
                if (keyInputStream != null ) {
                    try {
                        keyInputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (trustInputStream != null ) {
                    try {
                        trustInputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return privateService;
    }
}
