package com.ociweb.pronghorn.network;

import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.Appendables;

public class OAuth1HeaderBuilder {

  private static final String OAUTH_VERSION          = "oauth_version"; //optional for all calls
  
  private static final String OAUTH_NONCE            = "oauth_nonce"; //required for all calls
  private static final String OAUTH_TIMESTAMP        = "oauth_timestamp"; //required for all calls
  private static final String OAUTH_SIGNATURE_METHOD = "oauth_signature_method"; //required for all calls
  private static final String OAUTH_SIGNATURE        = "oauth_signature"; //required for all calls
  private static final String OAUTH_CONSUMER_KEY     = "oauth_consumer_key"; //required for all calls
  
  private static final String OAUTH_TOKEN            = "oauth_token"; //only for E and G
  private static final String OAUTH_VERIFIER         = "oauth_verifier";  // the pin only for E
  private static final String OAUTH_CALLBACK         = "oauth_callback"; //only for A
  
  private final SecureRandom secureRandom = new SecureRandom(); 
  
  private final StringBuilder nonceBuilder = new StringBuilder();
  private final StringBuilder timeBuilder = new StringBuilder();
  private final StringBuilder tokenBuilder = new StringBuilder();
  private final StringBuilder consumerKeyBuilder = new StringBuilder();
  private final StringBuilder callbackBuilder = new StringBuilder();
  private final StringBuilder verifierBuilder = new StringBuilder();
  
  
	
  private final List<CharSequence[]> macParams = new ArrayList<CharSequence[]>(); //in alpha order...  
  private final Pipe<RawDataSchema> workingPipe;    
  private final String formalPath;  
  private final Mac mac;
	
  //custom values
  private SecretKeySpec secretKeySpec;
  
  ///////////////////////
  //For a better understanding of A, E and G read the following
  //https://oauth.net/core/1.0/#anchor9
  ///////////////////////
  
  
  public OAuth1HeaderBuilder(int port, String scheme, String host, String path) {
   
    try {
		this.mac = Mac.getInstance("HmacSHA1");
	} catch (NoSuchAlgorithmException e) {
		throw new RuntimeException(e);
	}
   	
    consumerKeyBuilder.setLength(0);
	this.addMACParam(OAUTH_CONSUMER_KEY,consumerKeyBuilder); 
	
	this.addMACParam(OAUTH_NONCE,nonceBuilder);
	this.addMACParam(OAUTH_SIGNATURE_METHOD,"HMAC-SHA1");
	this.addMACParam(OAUTH_TIMESTAMP,timeBuilder);
	this.addMACParam(OAUTH_VERSION,"1.0");
		
	tokenBuilder.setLength(0);
	this.addMACParam(OAUTH_TOKEN,tokenBuilder);

	callbackBuilder.setLength(0);
	this.addMACParam(OAUTH_CALLBACK, callbackBuilder);
	
	verifierBuilder.setLength(0);
	this.addMACParam(OAUTH_VERIFIER, verifierBuilder);
	

	this.workingPipe = RawDataSchema.instance.newPipe(2, 1000);
	this.workingPipe.initBuffers();
	
	this.formalPath = buildFormalPath(port, scheme, host, path);

  }

  public void setupStep1(String consumerKey, String callback) {
	  // A
	  this.consumerKeyBuilder.setLength(0);
	  this.consumerKeyBuilder.append(consumerKey);
	  
	  assert(callback==null 
			  || callback.indexOf("%3")>0 
			  || callback.equals("oob")) : "value must be url encoded or set to oob for pin mode";
	  //example oauth_callback="http%3A%2F%2Fmyapp.com%3A3005%2Ftwitter%2Fprocess_callback"
	  
	  this.callbackBuilder.setLength(0);
	  if (null!=callback) {
		  this.callbackBuilder.append(callback);
	  }
	  
	  this.tokenBuilder.setLength(0);
	  this.secretKeySpec = new SecretKeySpec("anonymous&".getBytes(), "HmacSHA1");
  
	  //required for twitter 
	  //oauth_callback =
	  
//	  oauth_consumer_key:
//		  The Consumer Key.
//		  oauth_signature_method:
//		 	 The signature method the Consumer used to sign the request.
//		  oauth_signature:
//		  	The signature as defined in Signing Requests.
//		  oauth_timestamp:
//		 	 As defined in Nonce and Timestamp.
//		  oauth_nonce:
//		  	As defined in Nonce and Timestamp.
//		  oauth_version:
//		  	OPTIONAL. If present, value MUST be 1.0 . Service Providers MUST assume the protocol version to be 1.0 if this parameter is not present. Service Providers’ response to non-1.0 value is left undefined.
//		  Additional parameters:
//		 	 Any additional parameters, as defined by the Service Provider.
	  
	  ////////////
	  //responds with
	  ////////////
	  
//	  oauth_token:
//		  The Request Token.
//		  oauth_token_secret:
//		  		The Token Secret.
//		  Additional parameters:
//		  		Any additional parameters, as defined by the Service Provider.
  }
  
  public void setupStep2(String consumerKey, String consumerSecret, String token, String tokenSecret ) {
	  // E
	  this.consumerKeyBuilder.setLength(0);
	  this.consumerKeyBuilder.append(consumerKey);
	  this.tokenBuilder.setLength(0);
	  this.tokenBuilder.append(token);		
	  this.secretKeySpec = new SecretKeySpec((consumerSecret + "&").getBytes(), "HmacSHA1");
	 
//	  oauth_consumer_key:
//		  The Consumer Key.
//		  oauth_token:
//		  		The Request Token obtained previously.
//		  oauth_signature_method:
//		  		The signature method the Consumer used to sign the request.
//		  oauth_signature:
//		  		The signature as defined in Signing Requests.
//		  oauth_timestamp:
//		  		As defined in Nonce and Timestamp.
//		  oauth_nonce:
//		  		As defined in Nonce and Timestamp.
//		  oauth_version:
//		  		OPTIONAL. If present, value MUST be 1.0 . Service Providers MUST assume the protocol version to be 1.0 if this parameter is not present. Service Providers’ response to non-1.0 value is left undefined.
//	  
	  
	  
//	  oauth_token:
//		  The Access Token.
//		  oauth_token_secret:
//		  		The Token Secret.
//		  Additional parameters:
//		 		 Any additional parameters, as defined by the Service Provider.
  }
  
  public void setupStep3(String consumerKey, String consumerSecret, String token, String tokenSecret ) {
	 
	  this.consumerKeyBuilder.setLength(0);
	  this.consumerKeyBuilder.append(consumerKey);
	  this.tokenBuilder.setLength(0);
	  this.tokenBuilder.append(token);		
	  this.secretKeySpec = new SecretKeySpec((consumerSecret + "&" + tokenSecret).getBytes(), "HmacSHA1");
	  
  }
  
  public void addMACParam(CharSequence key, CharSequence dynamicValue) {
	  
	  try {
		  macParams.add(new CharSequence[]{
				  key,
				  dynamicValue
				  });
	  } catch (Exception e) {
		  throw new RuntimeException(e);
	  }
	  
	  Collections.sort(macParams, charComparitor);
  }

  public void addMACParam(String key, String value) {
	  
	  try {
		  macParams.add(new CharSequence[]{
				  URLEncoder.encode(key,"UTF-8"),
				  null==value?null:URLEncoder.encode(value,"UTF-8")
				  });
	  } catch (Exception e) {
		  throw new RuntimeException(e);
	  }
	  
	  Collections.sort(macParams, charComparitor);
  }
  
  private final int charCompare(CharSequence thisCS, CharSequence thatCS) {
      int len1 = thisCS.length();
      int len2 = thatCS.length();
      int lim = Math.min(len1, len2);
      int k = -1;
      while (++k < lim) {
          char c1 = thisCS.charAt(k);
          char c2 = thatCS.charAt(k);
          if (c1 != c2) {
              return c1 - c2;
          }
      }
      return len1 - len2;
  }
  
  private final Comparator<CharSequence[]> charComparitor = new Comparator<CharSequence[]>() {
	    @Override
	    public int compare(CharSequence[] thisPair, CharSequence[] thatPair) {
	      // sort params first by key, then by value
	      int keyCompare = charCompare(thisPair[0],thatPair[0]);
	      if (keyCompare == 0) {	    	  
	        return charCompare(thisPair[1],thatPair[1]);
	      } else {
	        return keyCompare;
	      }
	    }
  };
  
  
  public <A extends Appendable> A addHeaders(A builder, String upperVerb) {

	final long now = System.currentTimeMillis();
	
	nonceBuilder.setLength(0);
	Appendables.appendValue(nonceBuilder, Math.abs(secureRandom.nextLong()));
	Appendables.appendValue(nonceBuilder, now);

	timeBuilder.setLength(0);
	Appendables.appendValue(timeBuilder, now / 1000);
	
	///////////////////////////////////////
	//https://oauth.net/core/1.0/#anchor9
	//////////////////////
	//This call is part G
	//////////////////////////////////////
	
	try {
	    builder.append("Authorization: ");
	    
	    builder.append("OAuth ");
	    	    
	    if (callbackBuilder.length()>0) {
	    	builder.append(OAUTH_CALLBACK).append("=\"").append(callbackBuilder).append("\", ");
	    }
	    
	    builder.append(OAUTH_CONSUMER_KEY).append("=\"").append(consumerKeyBuilder).append("\", ");
	    
	    if (tokenBuilder.length()>0) {
	    	builder.append(OAUTH_TOKEN).append("=\"").append(tokenBuilder).append("\", ");
	    }
	    if (verifierBuilder.length()>0) {
	    	builder.append(OAUTH_VERIFIER).append("=\"").append(verifierBuilder).append("\", ");
	    }
	    mac.init(secretKeySpec);	    	    
		doFinalHMAC(builder.append(OAUTH_SIGNATURE).append("=\""), upperVerb, formalPath, mac);
		builder.append("\", ");
		
	    builder.append(OAUTH_SIGNATURE_METHOD).append("=\"").append("HMAC-SHA1").append("\", ");
	    builder.append(OAUTH_TIMESTAMP).append("=\"").append(timeBuilder).append("\", ");
	    builder.append(OAUTH_NONCE).append("=\"").append(nonceBuilder).append("\", ");
	    
	    builder.append(OAUTH_VERSION).append("=\"").append("1.0").append("\"");
	} catch (Exception e) {
		throw new RuntimeException(e);
	}
	return builder;
  }

    private <A extends Appendable> void doFinalHMAC(A builder, String upperVerb, String hostAndPath, Mac mac) {
	  int size = Pipe.addMsgIdx(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	  DataOutputBlobWriter<RawDataSchema> normalizedBuilder = Pipe.openOutputStream(workingPipe);
	  	  
	  normalizedBuilder.append(upperVerb);
	  normalizedBuilder.append('&');
	  normalizedBuilder.append(hostAndPath); //hostAndPath already URL encoded.
	  normalizedBuilder.append('&');
	
	  //NOTE: the params are already URL encoded.
	  if (!macParams.isEmpty()) {
		  
		boolean isFirst = true;  
	    for (int i=0; i<macParams.size(); i++) {
	      CharSequence[] pair = macParams.get(i);
	      CharSequence key = pair[0];
	      CharSequence value = pair[1];
	      
	      if (null!=value && value.length()>0) {
		      if (!isFirst) {
		    	  normalizedBuilder.append("%26");  
		    	  isFirst = false;
		      }
		      normalizedBuilder.append(key);
		      normalizedBuilder.append("%3D");
		      normalizedBuilder.append(value);  
	      }
	      
	    }
	    
	  }
	  DataOutputBlobWriter.closeLowLevelField(normalizedBuilder);
	  Pipe.confirmLowLevelWrite(workingPipe,size);
	  Pipe.publishWrites(workingPipe);
	
	  /////////////////////////////////////////
	  /////////////////////////////////////////
	
	  Pipe.takeMsgIdx(workingPipe);
	  
	  int meta = Pipe.takeRingByteMetaData(workingPipe);
	  int len  = Pipe.takeRingByteLen(workingPipe);
	  int mask = blobMask(workingPipe);	
      int pos = bytePosition(meta, workingPipe, len)&mask;     		
	  byte[] backing = byteBackingArray(meta, workingPipe);
	  
	  if ((pos+len) < workingPipe.sizeOfBlobRing) {
		  //single block
		  mac.update(backing, pos, len);
		  
	  } else {
		  //two blocks, this assumes the mac.update and doFinal can work "in place" which is true.
		  Pipe.copyBytesFromToRing(backing, pos, mask,
				  				   workingMacSpace, 0, Integer.MAX_VALUE, 
				  				   len);
		  mac.update(workingMacSpace, 0, len);
		  
	  }
	  
	  Pipe.confirmLowLevelRead(workingPipe, size);
	  Pipe.releaseReadLock(workingPipe);
	  
	  try {
		  
		  int macLen = mac.getMacLength();
		  mac.doFinal(workingMacSpace, 0);
		  Appendables.appendBase64Encoded(builder, workingMacSpace, 0, macLen, Integer.MAX_VALUE);
		  
		} catch (ShortBufferException e) {
			throw new RuntimeException(e);
		} catch (IllegalStateException e) {
			throw new RuntimeException(e);
		}
	
  }
  
  //temp mutable structure for building the mac upon request.
  private final byte[] workingMacSpace = new byte[1000];

  
  private String buildFormalPath(int port, String scheme, String host, String path) {
	
	StringBuilder requestUrlBuilder = new StringBuilder(512);
	  requestUrlBuilder.append(scheme.toLowerCase());
	  requestUrlBuilder.append("://");
	  requestUrlBuilder.append(host.toLowerCase());
	  if (includePortString(port, scheme)) {
	    requestUrlBuilder.append(":").append(port);
	  }
	  requestUrlBuilder.append(path);
	  String hostAndPath = requestUrlBuilder.toString();
	  
	  //Pre-URL encoded
	  try {
		return URLEncoder.encode(hostAndPath,"UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
}

  /**
     * The OAuth 1.0a spec says that the port should not be included in the normalized string
     * when (1) it is port 80 and the scheme is HTTP or (2) it is port 443 and the scheme is HTTPS
     */
    boolean includePortString(int port, String scheme) {
      return !((port == 80 && "HTTP".equalsIgnoreCase(scheme)) || (port == 443 && "HTTPS".equalsIgnoreCase(scheme)));
    }
}

