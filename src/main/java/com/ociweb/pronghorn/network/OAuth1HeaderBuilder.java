package com.ociweb.pronghorn.network;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.ociweb.pronghorn.util.Appendables;

public class OAuth1HeaderBuilder {

  private final String consumerKey;
  private final String consumerSecret;
  private final String token;
  private final String tokenSecret;

  private final SecureRandom secureRandom;
  
  public OAuth1HeaderBuilder(String consumerKey, String consumerSecret, String token, String tokenSecret) {
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    
    this.token = token;
    this.tokenSecret = tokenSecret;

    assert(consumerKey!=null);
    assert(consumerSecret!=null);
    assert(token!=null);
    assert(tokenSecret!=null);
    
    this.secureRandom = new SecureRandom();    
  }

  
  public void addHeaders(Appendable builder, List<CharSequence[]> params, String upperVerb, String hostAndPath) {
	long timestampSecs = generateTimestamp();
	String nonce = generateNonce();
	
	params.add(new CharSequence[]{"oauth_consumer_key",consumerKey}); 
	params.add(new CharSequence[]{"oauth_nonce",nonce}); 
	params.add(new CharSequence[]{"oauth_token",token});
	params.add(new CharSequence[]{"oauth_signature_method","HMAC-SHA1"});
	params.add(new CharSequence[]{"oauth_timestamp",Long.toString(timestampSecs)});
	params.add(new CharSequence[]{"oauth_version","1.0"});
 
	
	  Collections.sort(params, new Comparator<CharSequence[]>() {
	    @Override
	    public int compare(CharSequence[] thisPair, CharSequence[] thatPair) {
	      // sort params first by key, then by value
	      int keyCompare = ((String) thisPair[0]).compareTo((String) thatPair[0]); //TODO: bad cast!!
	      if (keyCompare == 0) {
	    	  
	        return ((String) thisPair[1]).compareTo((String) thatPair[1]); //TODO: bad cast!!
	      } else {
	        return keyCompare;
	      }
	    }
	  });
	
	  // We only need the stringbuilder for the duration of this method
	  StringBuilder paramsBuilder = new StringBuilder(512);
	  if (!params.isEmpty()) {
		CharSequence[] head = params.get(0);
	    paramsBuilder.append(head[0]).append('=').append(head[1]);
	    
	    for (int i=1; i<params.size(); i++) {
	      CharSequence[] pair = params.get(i);
	      paramsBuilder.append('&').append(pair[0]).append('=').append(pair[1]);
	    }
	  }
	
	
	  StringBuilder normalizedBuilder = new StringBuilder(512);
	
	  normalizedBuilder.append(upperVerb.toUpperCase());
	  try {
		normalizedBuilder.append('&').append(URLEncoder.encode(hostAndPath,"UTF-8"));
		normalizedBuilder.append('&').append(URLEncoder.encode(paramsBuilder.toString(),"UTF-8"));
	} catch (UnsupportedEncodingException e1) {
		throw new RuntimeException(e1);
	}
	
	//TODO: we have many places where this can be improved using pipes...
	byte[] utf8Bytes2 = normalizedBuilder.toString().getBytes(Charset.forName("UTF-8"));

	String key = consumerSecret + "&" + tokenSecret;
	byte[] utf8Bytes = key.getBytes(Charset.forName("UTF-8"));
	
	String signature;
	byte[] bytes;
	try {
	  Mac mac = Mac.getInstance("HmacSHA1");
	  mac.init(new SecretKeySpec(utf8Bytes, "HmacSHA1"));
	  bytes = mac.doFinal(utf8Bytes2);
	  
	  String temp = Appendables.appendBase64(new StringBuilder(), bytes, 0, bytes.length, Integer.MAX_VALUE).toString();
	  signature = URLEncoder.encode(temp,"UTF-8");

	} catch (Exception e) {
		throw new RuntimeException(e);
	}
	
	try {
	    builder.append("Authorization").append(": ");
	    
	    builder.append("OAuth ");
	    builder.append("oauth_consumer_key").append("=\"").append((consumerKey)).append("\", ");
	    builder.append("oauth_token").append("=\"").append((token)).append("\", ");
	    
	    builder.append("oauth_signature").append("=\"").append((signature)).append("\", ");
	    builder.append("oauth_signature_method").append("=\"").append(("HMAC-SHA1")).append("\", ");
	    builder.append("oauth_timestamp").append('=');
	    
	    Appendables.appendValue(builder, "\"", timestampSecs, "\", ");
	    
	    builder.append("oauth_nonce").append("=\"").append((nonce)).append("\", ");
	    builder.append("oauth_version").append("=\"").append("1.0").append("\"");
	} catch (Exception e) {
		throw new RuntimeException(e);
	}
}


public String buildFormalPath(int port, String scheme, String host, String path) {
	
	StringBuilder requestUrlBuilder = new StringBuilder(512);
	  requestUrlBuilder.append(scheme.toLowerCase());
	  requestUrlBuilder.append("://");
	  requestUrlBuilder.append(host.toLowerCase());
	  if (includePortString(port, scheme)) {
	    requestUrlBuilder.append(":").append(port);
	  }
	  requestUrlBuilder.append(path);
	  String hostAndPath = requestUrlBuilder.toString();
	  
	return hostAndPath;
}

  private long generateTimestamp() {
    long timestamp = System.currentTimeMillis();
    return timestamp / 1000;
  }

  private String generateNonce() {
    return Long.toString(Math.abs(secureRandom.nextLong())) + System.currentTimeMillis();
  }

  /**
     * The OAuth 1.0a spec says that the port should not be included in the normalized string
     * when (1) it is port 80 and the scheme is HTTP or (2) it is port 443 and the scheme is HTTPS
     */
    boolean includePortString(int port, String scheme) {
      return !((port == 80 && "HTTP".equalsIgnoreCase(scheme)) || (port == 443 && "HTTPS".equalsIgnoreCase(scheme)));
    }
	  

}

