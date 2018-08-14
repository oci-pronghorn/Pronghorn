package com.ociweb.pronghorn.util.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.util.ByteConsumer;

public class ByteConsumerCodePointConverter implements ByteConsumer {

	private static Logger logger = LoggerFactory.getLogger(ByteConsumerCodePointConverter.class);
	
	private ByteConsumer target;
	private int high10;//held for following call so we can combine them for the UTF-16 surrogate pair
	
	public ByteConsumerCodePointConverter() {
		setTarget(target);
	}
	
	public void setTarget(ByteConsumer target) {
		this.target = target;
	}
	
	@Override
	public void consume(byte[] backing, int pos, int len, int mask) {

		 int intValue = 0;
		 short c = 0;
		 int total = 4;
         while (--total>=0) {
        	 
                c = backing[mask & pos++];
                len--;
   
             	if ((c>='0') && (c<='9') ) {
             		intValue = (intValue<<4)+(c-'0'); 
             	} else  {
		               	c = (short)(c | 0x20);//to lower case
             		    if ((c>='a') && (c<='f') ) {
		                	 intValue = (intValue<<4)+(10+(c-'a'));
		               	} else {
		               		 logger.warn("unable to decode unicode");
			       			 target.consume((byte)0xFF);//replacement char to show that an error happened.
			    			 target.consume((byte)0xFD);
			    			 return;
		               	}
             	}
             
         }
         
         if ((intValue >= 0xD800) && (intValue<=0xDBFF )) {
        	 //Beginning of UTF-16 surrogate pairs detected
        	
        	 high10 = 0x3FF&(intValue - 0xD800);
        	 
        	 
        	 if (len!=0) {
        		 logger.warn("unable to decode unicode");
       			 target.consume((byte)0xFF);//replacement char to show that an error happened.
    			 target.consume((byte)0xFD);
        	 }
        	 //else do nothing because we are waiting for second call.
        	 return;
         } 
         if ((intValue >= 0xDC00) && (intValue<=0xDBFF )) {
        	 int low10 = intValue - 0xDC00;
        	 
        	 if (high10==-1) {
        		 logger.warn("unable to decode unicode");
        		 target.consume((byte)0xFF);//replacement char to show that an error happened.
    			 target.consume((byte)0xFD);
    			 high10 = -1;
    			 return;
        	 }
        	         	 
        	 intValue = (high10<<10) | low10;
        	 
        	 high10 = -1;
         }
                  
                   		
         encodeSingleChar(intValue, target);
         
         //send rest of data, if any
         if (len>0) {
         	target.consume(backing,pos,len,mask);
		 }
         
	}

	private static <S extends MessageSchema> void encodeSingleChar(int c, ByteConsumer target) {

	    if (c <= 0x007F) { // less than or equal to 7 bits or 127
	        // code point 7
	        target.consume( (byte) c);
	    } else {
	        if (c <= 0x07FF) { // less than or equal to 11 bits or 2047
	            // code point 11
	        	target.consume( (byte) (0xC0 | ((c >> 6) & 0x1F)));
	        } else {
	            if (c <= 0xFFFF) { // less than or equal to  16 bits or 65535

	            	//special case logic here because we know that c > 7FF and c <= FFFF so it may hit these
	            	// D800 through DFFF are reserved for UTF-16 and must be encoded as an 63 (error)
	            	if (0xD800 == (0xF800&c)) {
	            		target.consume( (byte)63 );
	            		return;
	            	}

	                // code point 16
	            	target.consume( (byte) (0xE0 | ((c >> 12) & 0x0F)));
	            } else {
	                rareEncodeCase(c, target);
	            }
	            target.consume( (byte) (0x80 | ((c >> 6) & 0x3F)));
	        }
	        target.consume( (byte) (0x80 | (c & 0x3F)));
	    }
	}

	private static <S extends MessageSchema> void rareEncodeCase(int c, ByteConsumer target) {
		if (c < 0x1FFFFF) {
		    // code point 21
			target.consume( (byte) (0xF0 | ((c >> 18) & 0x07)));
		} else {
		    if (c < 0x3FFFFFF) {
		        // code point 26
		    	target.consume((byte) (0xF8 | ((c >> 24) & 0x03)));
		    } else {
		        if (c < 0x7FFFFFFF) {
		            // code point 31
		        	target.consume( (byte) (0xFC | ((c >> 30) & 0x01)));
		        } else {
		            throw new UnsupportedOperationException("can not encode char with value: " + c);
		        }
		        target.consume( (byte) (0x80 | ((c >> 24) & 0x3F)));
		    }
		    target.consume( (byte) (0x80 | ((c >> 18) & 0x3F)));
		}
		target.consume((byte) (0x80 | ((c >> 12) & 0x3F)));

	}
	
	@Override
	public void consume(byte value) {
		target.consume(value);
	}

	
}
