package com.ociweb.pronghorn.network.config;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.module.FileReadModuleStage.FileReadModuleStageData;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTPSpecification  <   T extends Enum<T> & HTTPContentType,
                                    R extends Enum<R> & HTTPRevision,
                                    V extends Enum<V> & HTTPVerb,
                                    H extends Enum<H> & HTTPHeader
                                    > {
    
    public final Class<T> supportedHTTPContentTypes;
    public final Class<R> supportedHTTPRevisions;
    public final Class<V> supportedHTTPVerbs;
    public final Class<H> supportedHTTPHeaders;
    
    public final int maxVerbLength;
   
    public final int headerCount;
    public final H[] headers;
    public final T[] contentTypes;
    public final V[] verbs;
    public final R[] revisions;
    
    private static final Logger logger = LoggerFactory.getLogger(HTTPSpecification.class);
    
    private boolean trustAccurateStrings = true;
    private final TrieParser headerParser;
	private TrieParser contentTypeTrie;
	
	public final IntHashTable fileExtHashTable;
	public static final boolean supportWrongLineFeeds = false;

    //must not collide with any valid struct field ID so we start with min value and work up.
	public final static long END_OF_HEADER_ID  = Long.MIN_VALUE+1L;//for the empty header found at the bottom of the header
	public final static long UNKNOWN_HEADER_ID = Long.MIN_VALUE+2L;
    
    private static HTTPSpecification<HTTPContentTypeDefaults,HTTPRevisionDefaults,HTTPVerbDefaults,HTTPHeaderDefaults> defaultSpec;
    
    public static HTTPSpecification<HTTPContentTypeDefaults,HTTPRevisionDefaults,HTTPVerbDefaults,HTTPHeaderDefaults>  defaultSpec() {
        if (null == defaultSpec) {
            defaultSpec = new HTTPSpecification(HTTPContentTypeDefaults.class, HTTPRevisionDefaults.class, HTTPVerbDefaults.class,  HTTPHeaderDefaults.class );
        } 
        return defaultSpec;
    }

    public <A extends Appendable> A writeHeader(A target, int ordinal, ChannelReader data) {
    	return writeHeader(target, getHeader(ordinal), data);
    }

    public <A extends Appendable> A writeHeader(A target, HTTPHeader header, ChannelReader data) {
		try {
			target.append(header.writingRoot());
			header.writeValue(target, this, data);
		} catch (Throwable e) {
			logger.error("Bad header unable to parse {} ",header);
			throw new RuntimeException(e);
		}

		return target;
	}
    
    
    private HTTPSpecification(Class<T> supportedHTTPContentTypes, Class<R> supportedHTTPRevisions, Class<V> supportedHTTPVerbs, Class<H> supportedHTTPHeaders) {

        this.supportedHTTPContentTypes = supportedHTTPContentTypes;
        this.supportedHTTPRevisions = supportedHTTPRevisions;
        this.supportedHTTPVerbs = supportedHTTPVerbs;
        this.supportedHTTPHeaders = supportedHTTPHeaders;
        
        this.headers = supportedHTTPHeaders.getEnumConstants();
        this.headerCount = null==this.headers? 0 : headers.length;
   
        this.revisions = supportedHTTPRevisions.getEnumConstants();
        this.contentTypes = supportedHTTPContentTypes.getEnumConstants();
        
    	this.fileExtHashTable = buildFileExtHashTable(supportedHTTPContentTypes);
        this.contentTypeTrie = contentTypeTrieBuilder(contentTypes);
        
        //find ordinal values and max length
        int maxVerbLength = 0;
        this.verbs = supportedHTTPVerbs.getEnumConstants();
        if (this.verbs != null) {
	        int j = verbs.length;
	        while (--j >= 0) {        	
	        	maxVerbLength = Math.max(maxVerbLength, verbs[j].name().length());             
	        }
        }
        this.maxVerbLength = maxVerbLength;
        
        //build header lookup trie parser

        assert(false == (trustAccurateStrings=false)); //side effect by design, do not modify
        
        headerParser = new TrieParser(512, 2, trustAccurateStrings, false, true);        
        if (headers != null) {
	        int h = headers.length;
	        while (--h>=0) {	
	        	headerParser.setUTF8Value(headers[h].writingRoot(), headers[h].ordinal());
	        }
        }

        
    }
  
    public TrieParser headerParser() {
    	return headerParser;
    }
    
	public boolean headerMatches(int headerId, CharSequence cs) {
		return match(cs, headers[headerId].writingRoot());
	}

	public boolean verbMatches(int verbId, CharSequence cs) {
		return match(cs, verbs[verbId].getKey());
	}

	public boolean match(CharSequence a, CharSequence b) {
		if(b.length() != a.length()) {
			return false;
		}
		int i = b.length();
		while (--i>=0) {
			if (Character.toLowerCase(b.charAt(i))!= Character.toLowerCase(a.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	public int headerId(byte[] h, TrieParserReader reader) {
		return (int)reader.query(reader, headerParser(), h, 0, h.length, Integer.MAX_VALUE);
	}

	public H getHeader(int headerId) {
    	assert(this.headers!=null) : "No headers have been set in this specification.";
    	assert(headerId >= 0 && headerId < this.headers.length) : "There is no header with the provided ID";

    	return this.headers[headerId];
	}

	public HTTPContentType getContentType(int contentTypeId) {
    	assert(this.contentTypes!=null) : "No content types have been set in this specification.";
    	assert(contentTypeId >=0 && contentTypeId < this.contentTypes.length) : "There is no content type with the provided ID";

    	return this.contentTypes[contentTypeId];
	}

	public TrieParser contentTypeTrieBuilder() {
		return contentTypeTrie;
	}

	private TrieParser contentTypeTrieBuilder(HTTPContentType[] types) {
		  int x;
		  TrieParser typeMap = new TrieParser(4096,1,true,false,true);	//TODO: set switch to turn on off the deep check skip     TODO: must be shared across all instances?? 
	      if (null!=types) {
		      x = types.length;
		      while (--x >= 0) {
		    	  //with or without the  charset part on the end
		    	  //Content-Type: text/html; charset=ISO-8859-1 	  //;charset=utf-8 	 or ; charset=ISO-8859-1 
		    	  typeMap.setUTF8Value(types[x].contentType(),"\r\n", types[x].ordinal());	
		    	  typeMap.setUTF8Value(types[x].contentType(),"; charset=%b\r\n", types[x].ordinal());
		    	  typeMap.setUTF8Value(types[x].contentType(),";charset=%b\r\n", types[x].ordinal());
		    	  
		    	  typeMap.setUTF8Value(types[x].contentType(),"\n", types[x].ordinal());  //\n must be last because we prefer to have it pick \r\n
		    	  typeMap.setUTF8Value(types[x].contentType(),"; charset=%b\n", types[x].ordinal());
		    	  typeMap.setUTF8Value(types[x].contentType(),";charset=%b\n", types[x].ordinal());
		      }
	      }
		return typeMap;
	}

	public void visitHeaders(ChannelReader stream, Appendable target) {
		int id = stream.readShort();
		while (id > 0) {
			try {
				
				target.append(headers[id].writingRoot());
				
				headerConsume(id, stream, target);
				target.append("\n");
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			id = stream.readShort();
		}
		
	}
	
	
	public void headerSkip(int headerId, ChannelReader stream) {
		headers[headerId].skipValue(stream);		
	}

	public void headerConsume(int headerId, ChannelReader stream, Appendable target) {
		headers[headerId].consumeValue(stream, target);
		
	}

	public long headerConsume(int headerId, ChannelReader stream) {
		return headers[headerId].consumeValue(stream);
	}
	
	public static HTTPContentType lookupContentTypeByFullPathExtension(HTTPSpecification httpSpec, String resourceName) {
		int idxOfDot = resourceName.lastIndexOf('.');
		int typeIdx = 0;
		if (idxOfDot>=0) {
			int extHash = HTTPSpecification.extHash(resourceName.substring(idxOfDot+1, resourceName.length()));
			//value will be zero if not found
			typeIdx = IntHashTable.getItem(httpSpec.fileExtHashTable, extHash);
		}
		return (HTTPContentType)httpSpec.contentTypes[typeIdx];
	}
	
	public static HTTPContentType lookupContentTypeByExtension(HTTPSpecification httpSpec, String ext) {
		//value will be zero if not found
		return (HTTPContentType)httpSpec.contentTypes[IntHashTable.getItem(httpSpec.fileExtHashTable, HTTPSpecification.extHash(ext))];
	}
	

	private static < T extends Enum<T> & HTTPContentType> IntHashTable buildFileExtHashTable(Class<T> supportedHTTPContentTypes) {
	    int hashBits = 13; //8K
	    IntHashTable localExtTable = new IntHashTable(hashBits);
	    
	    T[] conentTypes = supportedHTTPContentTypes.getEnumConstants();
	    int c = conentTypes.length;
	    while (--c >= 0) {            
	        if (!conentTypes[c].isAlias()) {//never use an alias for the file Ext lookup.                
	            int hash = HTTPSpecification.extHash(conentTypes[c].fileExtension());
	            
	            if ( IntHashTable.hasItem(localExtTable, hash) ) {                
	                final int ord = IntHashTable.getItem(localExtTable, hash);
	                throw new UnsupportedOperationException("Hash error, check for new values and algo. "+conentTypes[c].fileExtension()+" colides with existing "+conentTypes[ord].fileExtension());                
	            } else {
	                IntHashTable.setItem(localExtTable, hash, conentTypes[c].ordinal());
	            }
	        }
	    }
	    return localExtTable;
	}

	public static int extHash(byte[] back, int pos, int len, int mask) {
	    int x = pos+len;
	    int result = back[mask&(x-1)];
	    int c;
	    while((--len >= 0) && ('.' != (c = back[--x & mask])) ) {   
	        result = (result << FileReadModuleStageData.extHashShift) ^ (0x1F & c); //mask to ignore sign                       
	    }        
	    return result;
	}

	public static int extHash(CharSequence cs) {
	    int len = cs.length();        
	    int result = cs.charAt(len-1);//init with the last value, will be used twice.    
	    while(--len >= 0) {
	        result = (result << FileReadModuleStageData.extHashShift) ^ (0x1F &  cs.charAt(len)); //mask to ignore sign    
	    }        
	    return result;
	}


    
}
