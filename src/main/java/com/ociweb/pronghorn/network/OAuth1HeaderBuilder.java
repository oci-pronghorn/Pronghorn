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

  private final String consumerKey;
  private final String token;
  
  private final SecureRandom secureRandom;
  private final SecretKeySpec secretKeySpec;
  
  private final StringBuilder nonceBuilder;
  private final StringBuilder timeBuilder;
	
  private final List<CharSequence[]> params;
  
  private final Pipe<RawDataSchema> workingPipe;
    
  private final String formalPath;
  
  public OAuth1HeaderBuilder(String consumerKey, String consumerSecret, String token, String tokenSecret,
		  					 int port, String scheme, String host, String path) {
    this.consumerKey = consumerKey;    
    this.token = token;

    assert(consumerKey!=null);
    assert(consumerSecret!=null);
    assert(token!=null);
    assert(tokenSecret!=null);
    
    this.secureRandom = new SecureRandom(); 
	this.secretKeySpec = new SecretKeySpec((consumerSecret + "&" + tokenSecret).getBytes(Charset.forName("UTF-8")), "HmacSHA1");
	
	this.nonceBuilder = new StringBuilder();
	this.timeBuilder = new StringBuilder();
	this.params = new ArrayList<CharSequence[]>(); //in alpha order...
	
	this.addParam("oauth_consumer_key",consumerKey); 
	this.addParam("oauth_nonce",nonceBuilder);
	this.addParam("oauth_signature_method","HMAC-SHA1");
	this.addParam("oauth_timestamp",timeBuilder);
	this.addParam("oauth_token",token);
	this.addParam("oauth_version","1.0");
	
	this.workingPipe = RawDataSchema.instance.newPipe(2, 1000);
	this.workingPipe.initBuffers();
	
	this.formalPath = buildFormalPath(port, scheme, host, path);

  }
  
  public void addParam(CharSequence key, CharSequence dynamicValue) {
	  
	  try {
		  assert(URLEncoder.encode(key.toString(),"UTF-8").equals(key)) : "key must not need encoding";
		  params.add(new CharSequence[]{
				  key,
				  dynamicValue
				  });
	  } catch (Exception e) {
		  throw new RuntimeException(e);
	  }
	  
	  Collections.sort(params, charComparitor);
  }

  public void addParam(String key, String value) {
	  
	  try {
		  params.add(new CharSequence[]{
				  URLEncoder.encode(key,"UTF-8"),
				  URLEncoder.encode(value,"UTF-8")
				  });
	  } catch (Exception e) {
		  throw new RuntimeException(e);
	  }
	  
	  Collections.sort(params, charComparitor);
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
	
	try {
	    builder.append("Authorization: ");
	    
	    builder.append("OAuth ");
	    builder.append("oauth_consumer_key=\"").append(consumerKey).append("\", ");
	    builder.append("oauth_token=\"").append(token).append("\", ");
	    
	    appendSignature(builder, upperVerb);
	    		
	    builder.append("oauth_signature_method=\"").append("HMAC-SHA1").append("\", ");
	    builder.append("oauth_timestamp=\"").append(timeBuilder).append("\", ");
	    
	    builder.append("oauth_nonce=\"").append(nonceBuilder).append("\", ");
	    builder.append("oauth_version=\"").append("1.0").append("\"");
	} catch (Exception e) {
		throw new RuntimeException(e);
	}
	return builder;
  }

	private <A extends Appendable> void appendSignature(A builder, String upperVerb)
			throws NoSuchAlgorithmException, InvalidKeyException, IOException {
		
		Mac mac = Mac.getInstance("HmacSHA1");
		mac.init(secretKeySpec);	    	    
		doFinalHMAC(builder.append("oauth_signature=\""), upperVerb, formalPath, mac);
		builder.append("\", ");
		
	}

  private <A extends Appendable> void doFinalHMAC(A builder, String upperVerb, String hostAndPath, Mac mac) {
	  int size = Pipe.addMsgIdx(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	  DataOutputBlobWriter<RawDataSchema> normalizedBuilder = Pipe.openOutputStream(workingPipe);
	  	  
	  normalizedBuilder.append(upperVerb);
	  normalizedBuilder.append('&');
	  normalizedBuilder.append(hostAndPath); //hostAndPath already URL encoded.
	  normalizedBuilder.append('&');
	
	  //NOTE: the params are already URL encoded.
	  if (!params.isEmpty()) {
		  
	    for (int i=0; i<params.size(); i++) {
	      CharSequence[] pair = params.get(i);
	      if (i>0) {
	    	  normalizedBuilder.append("%26");  
	      }
	      normalizedBuilder.append(pair[0]);
	      normalizedBuilder.append("%3D");
	      normalizedBuilder.append(pair[1]);  
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

