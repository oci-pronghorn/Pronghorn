package com.ociweb.pronghorn.ring;

import java.nio.ByteBuffer;



public class RingWriter {

	    
    public static void writeInt(RingBuffer rb, int value) {
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, value);        
    }
    
    public static void writeLong(RingBuffer rb, long value) {
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, (int)(value >>> 32), (int)value & 0xFFFFFFFF );    
    }

    public static void writeDecimal(RingBuffer rb, int exponent, long mantissa) {
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, exponent);   
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, (int) (mantissa >>> 32), (int)mantissa & 0xFFFFFFFF );    
    }

    
    //Because the stream needs to be safe and write the bytes ahead to the buffer we need 
    //to set the new byte pos, pos/len ints as a separate call
    public static void finishWriteBytesAlreadyStarted(RingBuffer rb, int p, int length) {
    	rb.validateVarLength(length);
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, p);
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, length);

        rb.byteWorkingHeadPos.value = p + length;
        
    }

    public static void writeBytes(RingBuffer rb, byte[] source) {
    	rb.validateVarLength(source.length);
        RingBuffer.addByteArray(source, 0, source.length, rb);
    }
    
	
    public static void writeBytes(RingBuffer rb, ByteBuffer source, int position, int length) {
    	rb.validateVarLength(length);
    	if ((position&rb.byteMask) > ((position+length-1)&rb.byteMask)) {
    		int temp = 1 + rb.mask - (position & rb.mask);
    		source.get(rb.byteBuffer, position & rb.byteMask, temp);
    		source.get(rb.byteBuffer, 0, length - temp);					    		
    	} else {					    	
    		source.get(rb.byteBuffer, position&rb.byteMask, length);
    	}
    	finishWriteBytesAlreadyStarted(rb, position, length);
    }
    
    
    public static void writeASCII(RingBuffer rb, char[] source) {
    	rb.validateVarLength(source.length);
        RingWriter.addASCIIToRing(source, 0, source.length, rb);
    }
    
    public static void writeASCII(RingBuffer rb, char[] source, int offset, int length) {
    	rb.validateVarLength(length);
        RingWriter.addASCIIToRing(source, offset, length, rb);
    }
    
    public static void writeASCII(RingBuffer rb, CharSequence source) {
    	rb.validateVarLength(source.length());
        RingWriter.addASCIIToRing(source, rb);
    }
    
    public static void writeUTF8(RingBuffer rb, char[] source) {
    	rb.validateVarLength(source.length<<3); //UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
        RingWriter.addUTF8ToRing(source, 0, source.length, rb);
    }
    
    public static void writeUTF8(RingBuffer rb, char[] source, int offset, int length) {
    	rb.validateVarLength(length<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
        RingWriter.addUTF8ToRing(source, offset, length, rb);
    }
    
    public static void writeUTF8(RingBuffer rb, CharSequence source) {
    	rb.validateVarLength(source.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
        RingWriter.addUTF8ToRing(source, rb);
    }

	private static void addASCIIToRing(char[] source, int sourceIdx, int sourceLen, RingBuffer rbRingBuffer) {
		
	    final int p = rbRingBuffer.byteWorkingHeadPos.value;
	    if (sourceLen > 0) {
	    	int targetMask = rbRingBuffer.byteMask;
	    	int proposedEnd = p + sourceLen;
			byte[] target = rbRingBuffer.byteBuffer;        	
			
	        int tStop = (p + sourceLen) & targetMask;
			int tStart = p & targetMask;
			if (tStop > tStart) {
				RingWriter.copyASCIIToByte(source, sourceIdx, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    int firstLen = 1+ targetMask - tStart;
			    RingWriter.copyASCIIToByte(source, sourceIdx, target, tStart, firstLen);
			    RingWriter.copyASCIIToByte(source, sourceIdx + firstLen, target, 0, sourceLen - firstLen);
			}
	        rbRingBuffer.byteWorkingHeadPos.value = proposedEnd;
	    }        
	    
	    RingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, p);
	    RingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, sourceLen);
	}

	private static void copyASCIIToByte(char[] source, int sourceIdx, byte[] target, int targetIdx, int len) {
		int i = len;
		while (--i>=0) {
			target[targetIdx+i] = (byte)(0xFF&source[sourceIdx+i]);
		}
	}
	
	private static void addASCIIToRing(CharSequence source, RingBuffer rbRingBuffer) {
		
	    final int p = rbRingBuffer.byteWorkingHeadPos.value;
	    int sourceLen = source.length();
	    if (sourceLen > 0) {
	    	int targetMask = rbRingBuffer.byteMask;
	    	int proposedEnd = p + sourceLen;
			byte[] target = rbRingBuffer.byteBuffer;        	
			
	        int tStop = (p + sourceLen) & targetMask;
			int tStart = p & targetMask;
			if (tStop > tStart) {
				RingWriter.copyASCIIToByte(source, 0, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    int firstLen = 1+ targetMask - tStart;
			    RingWriter.copyASCIIToByte(source, 0, target, tStart, firstLen);
			    RingWriter.copyASCIIToByte(source, firstLen, target, 0, sourceLen - firstLen);
			}
	        rbRingBuffer.byteWorkingHeadPos.value = proposedEnd;
	    }        
	    
	    RingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, p);
	    RingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, sourceLen);
	}

	
	private static void addUTF8ToRing(CharSequence source, RingBuffer rbRingBuffer) {
		
	    final int p = rbRingBuffer.byteWorkingHeadPos.value;
	    int sourceLen = source.length();
	    if (sourceLen > 0) {
	    	int targetMask = rbRingBuffer.byteMask;
	    	int proposedEnd = p + sourceLen;
			byte[] target = rbRingBuffer.byteBuffer;        	
			
	        int tStop = (p + sourceLen) & targetMask;
			int tStart = p & targetMask;
			if (tStop > tStart) {
				RingWriter.copyUTF8ToByte(source, 0, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    int firstLen = 1+ targetMask - tStart;
			    RingWriter.copyUTF8ToByte(source, 0, target, tStart, firstLen);
			    RingWriter.copyUTF8ToByte(source, firstLen, target, 0, sourceLen - firstLen);
			}
	        rbRingBuffer.byteWorkingHeadPos.value = proposedEnd;
	    }        
	    
	    RingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, p);
	    RingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, sourceLen);
	}
	
	private static void copyASCIIToByte(CharSequence source, int sourceIdx, byte[] target, int targetIdx, int len) {
		int i = len;
		while (--i>=0) {
			target[targetIdx+i] = (byte)(0xFF&source.charAt(sourceIdx+i));
		}
	}
	
	private static void copyUTF8ToByte(CharSequence source, int sourceIdx, byte[] target, int targetIdx, int len) {

        int pos = targetIdx;
        int c = 0;        
	    while (c < len) {
	        pos = RingWriter.encodeSingleChar((int) source.charAt(sourceIdx+c++), target, pos);
	    }		

	}
	
	private static void addUTF8ToRing(char[] source, int sourceIdx, int sourceLen, RingBuffer rbRingBuffer) {
		
	    final int p = rbRingBuffer.byteWorkingHeadPos.value;
	    if (sourceLen > 0) {
	    	int targetMask = rbRingBuffer.byteMask;
	    	int proposedEnd = p + sourceLen;
			byte[] target = rbRingBuffer.byteBuffer;        	
			
	        int tStop = (p + sourceLen) & targetMask;
			int tStart = p & targetMask;
			if (tStop > tStart) {
				RingWriter.copyUTF8ToByte(source, sourceIdx, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    int firstLen = 1+ targetMask - tStart;
			    RingWriter.copyUTF8ToByte(source, sourceIdx, target, tStart, firstLen);
			    RingWriter.copyUTF8ToByte(source, sourceIdx + firstLen, target, 0, sourceLen - firstLen);
			}
	        rbRingBuffer.byteWorkingHeadPos.value = proposedEnd;
	    }        
	    
	    RingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, p);
	    RingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, sourceLen);
	}
	
	private static void copyUTF8ToByte(char[] source, int sourceIdx, byte[] target, int targetIdx, int len) {

        int pos = targetIdx;
        int c = 0;        
	    while (c < len) {
	        pos = RingWriter.encodeSingleChar((int) source[sourceIdx+c++], target, pos);
	    }		

	}

	public static int encodeSingleChar(int c, byte[] buffer, int pos) {
	    if (c <= 0x007F) {
	        // code point 7
	        buffer[pos++] = (byte) c;
	    } else {
	        if (c <= 0x07FF) {
	            // code point 11
	            buffer[pos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
	        } else {
	            if (c <= 0xFFFF) {
	                // code point 16
	                buffer[pos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
	            } else {
	                if (c < 0x1FFFFF) {
	                    // code point 21
	                    buffer[pos++] = (byte) (0xF0 | ((c >> 18) & 0x07));
	                } else {
	                    if (c < 0x3FFFFFF) {
	                        // code point 26
	                        buffer[pos++] = (byte) (0xF8 | ((c >> 24) & 0x03));
	                    } else {
	                        if (c < 0x7FFFFFFF) {
	                            // code point 31
	                            buffer[pos++] = (byte) (0xFC | ((c >> 30) & 0x01));
	                        } else {
	                            throw new UnsupportedOperationException("can not encode char with value: " + c);
	                        }
	                        buffer[pos++] = (byte) (0x80 | ((c >> 24) & 0x3F));
	                    }
	                    buffer[pos++] = (byte) (0x80 | ((c >> 18) & 0x3F));
	                }
	                buffer[pos++] = (byte) (0x80 | ((c >> 12) & 0x3F));
	            }
	            buffer[pos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
	        }
	        buffer[pos++] = (byte) (0x80 | ((c) & 0x3F));
	    }
	    return pos;
	}
    
    
}
