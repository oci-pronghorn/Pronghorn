package com.ociweb.pronghorn.network;

import java.io.InputStream;

public interface TLSCertificates {
    InputStream keyInputStream();
    InputStream trustInputStream();
    String keyStorePassword();
    String keyPassword();
}
