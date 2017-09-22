package com.ociweb.pronghorn.network;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.ArrayList;
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
  private final SecretKeySpec secretKeySpec;
  
  private final StringBuilder nonceBuilder;
  private final StringBuilder timeBuilder;
	
  private final List<CharSequence[]> params;
  
  
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
	this.secretKeySpec = new SecretKeySpec((consumerSecret + "&" + tokenSecret).getBytes(Charset.forName("UTF-8")), "HmacSHA1");
	
	this.nonceBuilder = new StringBuilder();
	this.timeBuilder = new StringBuilder();
	this.params = new ArrayList<CharSequence[]>(); //in alpha order...
	this.params.add(new CharSequence[]{"oauth_consumer_key",consumerKey}); 
	this.params.add(new CharSequence[]{"oauth_nonce",nonceBuilder}); 
	this.params.add(new CharSequence[]{"oauth_signature_method","HMAC-SHA1"});
	this.params.add(new CharSequence[]{"oauth_timestamp",timeBuilder});
	this.params.add(new CharSequence[]{"oauth_token",token});
	this.params.add(new CharSequence[]{"oauth_version","1.0"});

  }

  public void addParam(CharSequence key, CharSequence value) {
	  params.add(new CharSequence[]{key,value});
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
  
  
  public <A extends Appendable> A addHeaders(A builder, String upperVerb, String hostAndPath) {

	final long now = System.currentTimeMillis();
	
	nonceBuilder.setLength(0);
	Appendables.appendValue(nonceBuilder, Math.abs(secureRandom.nextLong()));
	Appendables.appendValue(nonceBuilder, now);

	timeBuilder.setLength(0);
	Appendables.appendValue(timeBuilder, now / 1000);

	
	//TODO: ensure all stored values are encoded, then we do not need to do the encode here.
	
	  // We only need the stringbuilder for the duration of this method
	  StringBuilder paramsBuilder = new StringBuilder(512);
	  if (!params.isEmpty()) {		
	    for (int i=0; i<params.size(); i++) {
	      CharSequence[] pair = params.get(i);
	      paramsBuilder.append(pair[0]).append('=').append(pair[1]).append('&');
	    }
	    paramsBuilder.setLength(paramsBuilder.length()-1);
	  }
	  String encodedParams = null;
	  try {
		  encodedParams = URLEncoder.encode(paramsBuilder.toString(),"UTF-8");
	  } catch (UnsupportedEncodingException e1) {
		  throw new RuntimeException(e1);
	  }
	  	
	  
	  
	  StringBuilder normalizedBuilder = new StringBuilder(512);	
	  normalizedBuilder.append(upperVerb.toUpperCase());
	  normalizedBuilder.append('&').append(hostAndPath); //hostAndPath already URL encoded.
	  normalizedBuilder.append('&').append(encodedParams);
		
	//TODO: we have many places where this can be improved using pipes...
	byte[] utf8Bytes2 = normalizedBuilder.toString().getBytes(Charset.forName("UTF-8"));

	//TODO: we have many places where this can be improved using pipes...

	byte[] bytes;
	try {
	  Mac mac = Mac.getInstance("HmacSHA1");
	  mac.init(secretKeySpec);
	  bytes = mac.doFinal(utf8Bytes2);	
	} catch (Exception e) {
		throw new RuntimeException(e);
	}
	
	try {
	    builder.append("Authorization: ");
	    
	    builder.append("OAuth ");
	    builder.append("oauth_consumer_key").append("=\"").append((consumerKey)).append("\", ");
	    builder.append("oauth_token").append("=\"").append((token)).append("\", ");
	    	    
		Appendables.appendBase64Encoded(builder.append("oauth_signature").append("=\""),
				bytes, 0, bytes.length, Integer.MAX_VALUE).append("\", ");
	    
	    builder.append("oauth_signature_method").append("=\"").append(("HMAC-SHA1")).append("\", ");
	    builder.append("oauth_timestamp").append('=');
	    builder.append("\"").append(timeBuilder).append("\", ");
	    
	    builder.append("oauth_nonce").append("=\"").append(nonceBuilder).append("\", ");
	    builder.append("oauth_version").append("=\"").append("1.0").append("\"");
	} catch (Exception e) {
		throw new RuntimeException(e);
	}
	return builder;
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

