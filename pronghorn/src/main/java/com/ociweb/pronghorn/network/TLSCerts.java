package com.ociweb.pronghorn.network;

public class TLSCerts implements TLSCertificates {

	
	//pre-populated with default values
	private String identityStoreResourceName = 
			TLSCertificates.defaultCerts.identityStoreResourceName();
	
	private String trustStoreResourceName = 
			TLSCertificates.defaultCerts.trustStoreResourceName();
	
	private String keyStorePassword = 
			TLSCertificates.defaultCerts.keyStorePassword();
	
	private String keyPassword = 
			TLSCertificates.defaultCerts.keyPassword();
	
	private boolean trustAllCerts = 
			TLSCertificates.defaultCerts.trustAllCerts();
	
	private boolean clientAuthRequired =
			TLSCertificates.defaultCerts.clientAuthRequired();

	
	private boolean mutable = true;

	
	public static TLSCerts define() {
		return new TLSCerts();
	}
	
	public static TLSCerts trustAll() {
		return new TLSCerts().trustAllCerts(true);
	}
	
	public TLSCerts identityStoreResourceName(String resourcePath) {
		if (mutable) {
			this.identityStoreResourceName = resourcePath;
			return this;
		} else {
			throw new UnsupportedOperationException();
		}
	}
	
	public TLSCerts trustStoreResourceName(String resourcePath) {
		if (mutable) {
			this.trustStoreResourceName = resourcePath;			
			return this;
		} else {
			throw new UnsupportedOperationException();
		}
	}
	
	public TLSCerts keyStorePassword(String password) {
		if (mutable) {
			this.keyStorePassword = password;			
			return this;
		} else {
			throw new UnsupportedOperationException();
		}
	}
	
	public TLSCerts keyPassword(String password) {
		if (mutable) {
			this.keyPassword = password;			
			return this;
		} else {
			throw new UnsupportedOperationException();
		}
	}
	
	public TLSCerts trustAllCerts(boolean trustAllCerts) {
		if (mutable) {
			this.trustAllCerts = trustAllCerts;
			return this;
		} else {
			throw new UnsupportedOperationException();
		}
	}
	
	public TLSCerts clientAuthRequired(boolean clientAuthRequired) {
		if (mutable) {
			this.clientAuthRequired = clientAuthRequired;
			return this;
		} else {
			throw new UnsupportedOperationException();
		}
	}	

	@Override
	public String identityStoreResourceName() {
		mutable = false;
		return identityStoreResourceName;
	}

	@Override
	public String keyStorePassword() {
		mutable = false;
		return keyStorePassword;
	}

	@Override
	public String trustStoreResourceName() {
		mutable = false;
		return trustStoreResourceName;
	}

	@Override
	public String keyPassword() {
		mutable = false;
		return keyPassword;
	}

	@Override
	public boolean trustAllCerts() {
		mutable = false;
		return trustAllCerts;
	}

	@Override
	public boolean clientAuthRequired() {
		mutable = false;
		return clientAuthRequired;
	}
	
	
}
