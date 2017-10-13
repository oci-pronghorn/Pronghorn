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

  ////////////////////////////
  //The notes about A E and G are more clear  
  //if you have this page open https://oauth.net/core/1.0/#anchor9
  ////////////////////////////
	
  private static final String OAUTH_VERIFIER         = "oauth_verifier";
  private static final String OAUTH_CALLBACK         = "oauth_callback";
  private static final String OAUTH_SIGNATURE        = "oauth_signature";
  private static final String OAUTH_VERSION          = "oauth_version";
  private static final String OAUTH_TOKEN            = "oauth_token";
  private static final String OAUTH_TIMESTAMP        = "oauth_timestamp";
  private static final String OAUTH_SIGNATURE_METHOD = "oauth_signature_method";
  private static final String OAUTH_NONCE            = "oauth_nonce";
  private static final String OAUTH_CONSUMER_KEY     = "oauth_consumer_key";

  private final Mac mac;  
  private final String consumerKey;
  
  //used by E and G
  private CharSequence token;    //when null OAUTH_TOKEN field is not used
  private CharSequence consumerSecret;
  private CharSequence tokenSecret;
  
  
  //used by E
  private CharSequence verifier; //when null OAUTH_VERIFIER field is not used
  
  //used by A
  private CharSequence callback; //when null OAUTH_CALLBACK field is not used
  
  private final SecureRandom secureRandom;
  
  private final SecretKeySpec secretKeySpec; //need 3 of these TODO: fix...
  
  private final StringBuilder nonceBuilder;
  private final StringBuilder timeBuilder;
	
  private final List<CharSequence[]> macParams;
  
  private final Pipe<RawDataSchema> workingPipe;
  private final String formalPath;

  

    
  
  public OAuth1HeaderBuilder(String consumerKey,    //oauth_consumer_key - Not a secret (user)
		  		  
		                     String consumerSecret,  //?? where from? is F secret?		                     
		                     String token,           //from A and F   //oauth_token    - Not a secret (app) 
		                     String tokenSecret,     //from A and F               
		                     
		  					 int port, String scheme,
		  					 String host, 
		  					 String path) {
	  
	  this.macParams = new ArrayList<CharSequence[]>(); //in alpha order...
	  
      this.consumerKey = consumerKey;
    
      this.token = token;

    
    
    
    assert(consumerKey!=null);
    assert(consumerSecret!=null);
    assert(token!=null);
    assert(tokenSecret!=null);
    this.secretKeySpec = new SecretKeySpec((consumerSecret + "&" + tokenSecret).getBytes(), "HmacSHA1");
    
    
    try {
		mac = Mac.getInstance("HmacSHA1");
	} catch (NoSuchAlgorithmException e) {
		throw new RuntimeException(e);
	}
    
    //oauth_verifier is the pin
    //what are the signaature rules for A E and G??
    
    this.secureRandom = new SecureRandom();
	this.nonceBuilder = new StringBuilder();
	this.timeBuilder = new StringBuilder();
	
	//oauth_signature //required for all calls but not here in params since params is what it signs
	this.addMACParam(OAUTH_CONSUMER_KEY,consumerKey); //required for all calls
	this.addMACParam(OAUTH_NONCE,nonceBuilder); //required for all calls
	this.addMACParam(OAUTH_SIGNATURE_METHOD,"HMAC-SHA1"); //required for all calls
	this.addMACParam(OAUTH_TIMESTAMP,timeBuilder);//required for all calls	
	this.addMACParam(OAUTH_VERSION,"1.0"); //optional for all calls

	this.addMACParam(OAUTH_TOKEN, token); //only for E and G, null eliminates usage	
	this.addMACParam(OAUTH_CALLBACK, null);  //only for A, null eliminates usage	
	this.addMACParam(OAUTH_VERIFIER, null);  //only for E, null eliminates usage	
	
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
				  URLEncoder.encode(value,"UTF-8")
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
	
	///////////////////////////////
	////////get fresh NONCE
	/////////////////////////////
	nonceBuilder.setLength(0);
	Appendables.appendValue(nonceBuilder, Math.abs(secureRandom.nextLong()));
	Appendables.appendValue(nonceBuilder, now);
///////////////////////////////////////////
	
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
	    
	    builder.append(OAUTH_CONSUMER_KEY);// //required for all calls
	    builder.append("=\"").append(consumerKey).append("\", ");
	    
	    builder.append(OAUTH_TOKEN); //only for E and G
	    builder.append("=\"").append(token).append("\", ");
	    	    
	    mac.init(secretKeySpec); //choose right secret for A E or G
	    
	    
		doFinalHMAC(builder.append(OAUTH_SIGNATURE)// //required for all calls
				.append("=\""), upperVerb, formalPath, mac).append("\", ");
	   
	    builder.append(OAUTH_SIGNATURE_METHOD);// //required for all calls
	    builder.append("=\"").append("HMAC-SHA1").append("\", ");
	    
	    builder.append(OAUTH_TIMESTAMP);// //required for all calls
	    builder.append("=\"").append(timeBuilder).append("\", ");
	    
	    builder.append(OAUTH_NONCE);// //required for all calls
	    builder.append("=\"").append(nonceBuilder).append("\", ");
	    
	    builder.append(OAUTH_VERSION);// //optional for all calls
	    builder.append("=\"").append("1.0").append("\"");
	
	} catch (Exception e) {
		throw new RuntimeException(e);
	}
	return builder;
  }

	private <A extends Appendable> A doFinalHMAC(A builder, String upperVerb, String hostAndPath, Mac mac) {
	  int size = Pipe.addMsgIdx(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	  DataOutputBlobWriter<RawDataSchema> normalizedBuilder = Pipe.openOutputStream(workingPipe);
	  	  
	  normalizedBuilder.append(upperVerb);
	  normalizedBuilder.append('&');
	  normalizedBuilder.append(hostAndPath); //hostAndPath already URL encoded.
	  normalizedBuilder.append('&');
	
	  //NOTE: the params are already URL encoded.
	  if (!macParams.isEmpty()) {
		  
	    for (int i=0; i<macParams.size(); i++) {
	      CharSequence[] pair = macParams.get(i);
	      //NOTE: if value is null or zero length we do not use it because
	      //      those fields do not apply to this particular communication trip
	      CharSequence value = pair[1];
		  if (null != value && value.length()>0) {
		      if (i>0) {
		    	  normalizedBuilder.append("%26");  
		      }
		      normalizedBuilder.append(pair[0]);
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
	return builder;
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

