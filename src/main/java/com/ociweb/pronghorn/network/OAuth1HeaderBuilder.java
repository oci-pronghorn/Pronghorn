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

  private static final String OAUTH_VERSION          = "oauth_version";
  private static final String OAUTH_NONCE            = "oauth_nonce";
  private static final String OAUTH_TIMESTAMP        = "oauth_timestamp";
  private static final String OAUTH_SIGNATURE_METHOD = "oauth_signature_method";
  private static final String OAUTH_SIGNATURE        = "oauth_signature";
  private static final String OAUTH_TOKEN            = "oauth_token";
  private static final String OAUTH_CONSUMER_KEY     = "oauth_consumer_key";


  private final String consumerKey;
  
  private final SecureRandom secureRandom;
  private final SecretKeySpec secretKeySpec;
  
  private final StringBuilder nonceBuilder;
  private final StringBuilder timeBuilder;
	
  private final List<CharSequence[]> macParams;  
  private final Pipe<RawDataSchema> workingPipe;    
  private final String formalPath;  
  private final Mac mac;
	
  //custom values
  private String token;
  private String consumerSecret;
  private String tokenSecret;
  
  
  
  public OAuth1HeaderBuilder(String consumerKey,    //oauth_consumer_key - Not a secret (user)
		  
		                     String consumerSecret, 
		                     String token,          //oauth_token    - Not a secret (app) 
		                     String tokenSecret,
		  
		                     int port, String scheme, String host, String path) {
    this.consumerKey = consumerKey;    
    this.token = token;

    try {
		this.mac = Mac.getInstance("HmacSHA1");
	} catch (NoSuchAlgorithmException e) {
		throw new RuntimeException(e);
	}
    
    assert(consumerKey!=null);
    assert(consumerSecret!=null);
    assert(token!=null);
    assert(tokenSecret!=null);
    
    //oauth_verifier is the pin
    
    this.secureRandom = new SecureRandom(); 
	this.secretKeySpec = new SecretKeySpec((consumerSecret + "&" + tokenSecret).getBytes(), "HmacSHA1");
	
	this.nonceBuilder = new StringBuilder();
	this.timeBuilder = new StringBuilder();
	this.macParams = new ArrayList<CharSequence[]>(); //in alpha order...
	
	this.addMACParam(OAUTH_CONSUMER_KEY,consumerKey); 
	this.addMACParam(OAUTH_NONCE,nonceBuilder);
	this.addMACParam(OAUTH_SIGNATURE_METHOD,"HMAC-SHA1");
	this.addMACParam(OAUTH_TIMESTAMP,timeBuilder);
	this.addMACParam(OAUTH_VERSION,"1.0");
	
	this.addMACParam(OAUTH_TOKEN,token);

	this.workingPipe = RawDataSchema.instance.newPipe(2, 1000);
	this.workingPipe.initBuffers();
	
	this.formalPath = buildFormalPath(port, scheme, host, path);

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
	    builder.append(OAUTH_CONSUMER_KEY).append("=\"").append(consumerKey).append("\", ");
	    
	    if (null!=token) {
	    	builder.append(OAUTH_TOKEN).append("=\"").append(token).append("\", ");
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
	      
	      if (null!=value) {
		      if (!isFirst) {
		    	  normalizedBuilder.append("%26");  
		      }
		      normalizedBuilder.append(key);
		      normalizedBuilder.append("%3D");
		      normalizedBuilder.append(value);  
		      isFirst = false;
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

