package com.ociweb.pronghorn.ring;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.ociweb.pronghorn.ring.util.PaddedAtomicInteger;
import com.ociweb.pronghorn.ring.util.PaddedAtomicLong;



/**
 * Specialized ring buffer for holding decoded values from a FAST stream. Ring
 * buffer has blocks written which correspond to whole messages or sequence
 * items. Within these blocks the consumer is provided random (eg. direct)
 * access capabilities.
 * 
 * 
 * 
 * @author Nathan Tippy
 * 
 * 
 * Storage:
 *  int - 1 slot
 *  long - 2 slots, high then low 
 *  text - 2 slots, index then length  (if index is negative use constant array)
 * 
 */

// TODO: C, add map method which can take data from one ring buffer and populate another.
// TODO: C, look at adding reduce method in addition to filter.
// TODO: X, dev ops tool to empty (drain) buffers and record the loss.
// TODO: B, must add way of selecting what field to skip writing for the consumer.

//TODO: B, build  null ring buffer to drop messages.


public final class RingBuffer {
   
    public static class PaddedLong {
        public long value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;
    }
    
    public static class PaddedInt {
        public int value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;
    }
    
    //TODO: AA, note original disrupter allows for multiple threads to each visit the same spot and each do mutation
    //          there is no problem with doing this upgrade to the ring buffer support.
    
    
    public final int maxSize;
    public final int[] buffer;
    public final int mask;
    public final PaddedLong workingHeadPos = new PaddedLong();
    public final PaddedLong workingTailPos = new PaddedLong();

    public final AtomicLong tailPos = new PaddedAtomicLong(); // producer is allowed to write up to tailPos
    public final AtomicLong headPos = new PaddedAtomicLong(); // consumer is allowed to read up to headPos

    public final int maxByteSize;
    public final byte[] byteBuffer;
    public final int byteMask;
    
    public final PaddedInt byteWorkingHeadPos = new PaddedInt();
    public final PaddedInt byteWorkingTailPos = new PaddedInt();
        
    public final PaddedAtomicInteger bytesHeadPos = new PaddedAtomicInteger();
    public final PaddedAtomicInteger bytesTailPos = new PaddedAtomicInteger();
    
    //defined externally and never changes
    final byte[] constByteBuffer;
    private final byte[][] bufferLookup;
    
    public final int maxAvgVarLen; 
    private int varLenMovingAverage = 0;//this is an exponential moving average

    // end of moveNextFields

    static final int JUMP_MASK = 0xFFFFF;
    public final RingWalker consumerData;
    
    public final byte pBits;
    public final byte bBits;
    
    private final AtomicBoolean shutDown = new AtomicBoolean(false);//TODO: A, create unit test examples for using this.
	boolean writeTrailingCountOfBytesConsumed;
    
    
    public RingBuffer(RingBufferConfig config) {
    	this(config.primaryBits, config.byteBits, config.byteConst, config.from);
    }
    
    /**
     * Construct simple ring buffer without any assumed data structures
     * @param primaryBits
     * @param byteBits
     */
    public RingBuffer(byte primaryBits, byte byteBits) {
    	this(primaryBits, byteBits, null,  FieldReferenceOffsetManager.RAW_BYTES);
    	if ((primaryBits>>1)>byteBits) {
    		throw new UnsupportedOperationException("The byteBits value must be at least "+(primaryBits>>1)+" and should be even bigger but it was set to "+byteBits+" alternatively primaryBits could be set to a value less than "+(byteBits<<1));
    	}    	
    }
    
    
    /**
     * Construct ring buffer with re-usable constants and fragment structures
     * 
     * @param primaryBits
     * @param byteBits
     * @param byteConstants
     * @param from
     */
    public RingBuffer(byte primaryBits, byte byteBits, byte[] byteConstants, FieldReferenceOffsetManager from) {
        //constant data will never change and is populated externally.
        
    	this.pBits = primaryBits;
    	this.bBits = byteBits;
    	
        assert (primaryBits >= 0); //zero is a special case for a mock ring       
                
        //single buffer size for every nested set of groups, must be set to support the largest need.
        this.maxSize = 1 << primaryBits;
        this.mask = maxSize - 1;
        
        this.buffer = new int[maxSize];    
  
        //single text and byte buffers because this is where the variable length data will go.

        this.maxByteSize =  1 << byteBits;
        this.byteMask = maxByteSize - 1;
        this.byteBuffer = new byte[maxByteSize];

        this.constByteBuffer = byteConstants;
        this.bufferLookup = new byte[][] {byteBuffer,constByteBuffer};
                
        this.consumerData = new RingWalker(mask, from);
        
        if (0 == from.maxVarFieldPerUnit || 0==primaryBits) { //zero bits is for the dummy mock case
        	maxAvgVarLen = 0; //no fragments had any variable length fields so we never allow any
        } else {
        	//given outer ring buffer this is the maximum number of var fields that can exist at the same time.
        	int maxVarCount = (int)Math.ceil(maxSize*from.maxVarFieldPerUnit);
        	//we require at least 2 fields to ensure that the average approach works in all cases
        	if (maxVarCount < 2) {
        		// 2 = size * perUnit
        		int minSize = (int)Math.ceil(2f/from.maxVarFieldPerUnit);
        		int minBits = 32 - Integer.numberOfLeadingZeros(minSize - 1);
        		throw new UnsupportedOperationException("primary buffer is too small it must be at least "+minBits+" bits"); 
        	}
        	//to allow more almost 2x more flexibility in variable length bytes we track pairs of writes and ensure the 
        	//two together are below the threshold rather than each alone
        	maxAvgVarLen = maxByteSize/maxVarCount;
        }
        
             
        
    }
    
	public static void validateVarLength(RingBuffer rb, int length) {
		int newAvg = (length+rb.varLenMovingAverage)>>1;
        if (newAvg>rb.maxAvgVarLen)	{
            //compute some helpful information to add to the exception    	
        	int bytesPerInt = (int)Math.ceil(length*RingBuffer.from(rb).maxVarFieldPerUnit);
        	int bitsDif = 32 - Integer.numberOfLeadingZeros(bytesPerInt - 1);
        	
        	throw new UnsupportedOperationException("Can not write byte array of length "+length+". The dif between primary and byte bits should be at least "+bitsDif+". "+rb.pBits+","+rb.bBits);
        }
        rb.varLenMovingAverage = newAvg;
	}

    
    /**
     * Empty and restore to original values.
     */
    public void reset() {

    	workingHeadPos.value = 0;
        workingTailPos.value = 0;
        tailPos.set(0);
        headPos.set(0); 
        byteWorkingHeadPos.value = 0;
        bytesHeadPos.set(0);
        byteWorkingTailPos.value = 0;
        bytesTailPos.set(0);
        
        RingWalker.reset(consumerData, 0);
    }
        
    /**
     * Rest to desired position, helpful in unit testing to force wrap off the end.
     * @param toPos
     */
    public void reset(int toPos, int bPos) {

    	workingHeadPos.value = toPos;
        workingTailPos.value = toPos;
        tailPos.set(toPos);
        headPos.set(toPos); 
        byteWorkingHeadPos.value = bPos;
        bytesHeadPos.set(bPos);
        byteWorkingTailPos.value = bPos;
        bytesTailPos.set(bPos);
        
        RingWalker.reset(consumerData, toPos);
    }

	public static void copyBytesFromToRing(byte[] source, int sourceloc, int sourceMask, byte[] target, int targetloc, int targetMask, int length) {
		copyBytesFromToRingMasked(source, sourceloc & sourceMask, (sourceloc + length) & sourceMask, target, targetloc & targetMask, (targetloc + length) & targetMask,	length);
	}

	public static void copyIntsFromToRing(int[] source, int sourceloc, int sourceMask, int[] target, int targetloc, int targetMask, int length) {
		copyIntsFromToRingMasked(source, sourceloc & sourceMask, (sourceloc + length) & sourceMask, target, targetloc & targetMask, (targetloc + length) & targetMask, length);
	}

	
	private static void copyBytesFromToRingMasked(byte[] source,
			final int rStart, final int rStop, byte[] target, final int tStart,
			final int tStop, int length) {
		if (tStop >= tStart) {
			doubleMaskTargetDoesNotWrap(source, rStart, rStop, target, tStart, length);    			
		} else {
			doubleMaskTargetWraps(source, rStart, rStop, target, tStart, tStop,	length);
		}
	}


	private static void copyIntsFromToRingMasked(int[] source,
			final int rStart, final int rStop, int[] target, final int tStart,
			final int tStop, int length) {
		if (tStop >= tStart) {
			doubleMaskTargetDoesNotWrap(source, rStart, rStop, target, tStart, length);    			
		} else {
			doubleMaskTargetWraps(source, rStart, rStop, target, tStart, tStop,	length);
		}
	}

	private static void doubleMaskTargetDoesNotWrap(byte[] source,
			final int rStart, final int rStop, byte[] target, final int tStart,
			int length) {
		if (rStop >= rStart) {
			//the source and target do not wrap
			System.arraycopy(source, rStart, target, tStart, length);
		} else {
			//the source is wrapping but not the target
			System.arraycopy(source, rStart, target, tStart, length-rStop);
			System.arraycopy(source, 0, target, tStart + length - rStop, rStop);
		}
	}

	private static void doubleMaskTargetDoesNotWrap(int[] source,
			final int rStart, final int rStop, int[] target, final int tStart,
			int length) {
		if (rStop >= rStart) {
			//the source and target do not wrap
			System.arraycopy(source, rStart, target, tStart, length);
		} else {
			//the source is wrapping but not the target
			System.arraycopy(source, rStart, target, tStart, length-rStop);
			System.arraycopy(source, 0, target, tStart + length - rStop, rStop);
		}
	}
	
	private static void doubleMaskTargetWraps(byte[] source, final int rStart,
			final int rStop, byte[] target, final int tStart, final int tStop,
			int length) {
		if (rStop >= rStart) {
//				//the source does not wrap but the target does
//				// done as two copies
		    System.arraycopy(source, rStart, target, tStart, length-tStop);
		    System.arraycopy(source, rStart + length - tStop, target, 0, tStop);
		} else {
		    if (length>0) {
				//both the target and the source wrap
		    	doubleMaskDoubleWrap(source, target, length, tStart, rStart, length-tStop, length-rStop);
			}
		}
	}
	
	private static void doubleMaskTargetWraps(int[] source, final int rStart,
			final int rStop, int[] target, final int tStart, final int tStop,
			int length) {
		if (rStop >= rStart) {
//				//the source does not wrap but the target does
//				// done as two copies
		    System.arraycopy(source, rStart, target, tStart, length-tStop);
		    System.arraycopy(source, rStart + length - tStop, target, 0, tStop);
		} else {
		    if (length>0) {
				//both the target and the source wrap
		    	doubleMaskDoubleWrap(source, target, length, tStart, rStart, length-tStop, length-rStop);
			}
		}
	}

	private static void doubleMaskDoubleWrap(byte[] source, byte[] target,
			int length, final int tStart, final int rStart, int targFirstLen,
			int srcFirstLen) {
		if (srcFirstLen<targFirstLen) {
			//split on src first
			System.arraycopy(source, rStart, target, tStart, srcFirstLen);
			System.arraycopy(source, 0, target, tStart+srcFirstLen, targFirstLen - srcFirstLen);
			System.arraycopy(source, targFirstLen - srcFirstLen, target, 0, length - targFirstLen);    			    	
		} else {
			//split on targ first
			System.arraycopy(source, rStart, target, tStart, targFirstLen);
			System.arraycopy(source, rStart + targFirstLen, target, 0, srcFirstLen - targFirstLen); 
			System.arraycopy(source, 0, target, srcFirstLen - targFirstLen, length - srcFirstLen);
		}
	}
	
	private static void doubleMaskDoubleWrap(int[] source, int[] target,
			int length, final int tStart, final int rStart, int targFirstLen,
			int srcFirstLen) {
		if (srcFirstLen<targFirstLen) {
			//split on src first
			System.arraycopy(source, rStart, target, tStart, srcFirstLen);
			System.arraycopy(source, 0, target, tStart+srcFirstLen, targFirstLen - srcFirstLen);
			System.arraycopy(source, targFirstLen - srcFirstLen, target, 0, length - targFirstLen);    			    	
		} else {
			//split on targ first
			System.arraycopy(source, rStart, target, tStart, targFirstLen);
			System.arraycopy(source, rStart + targFirstLen, target, 0, srcFirstLen - targFirstLen); 
			System.arraycopy(source, 0, target, srcFirstLen - targFirstLen, length - srcFirstLen);
		}
	}

	public static int leftConvertIntToASCII(RingBuffer rb, int value, int idx) {
		//max places is value for -2B therefore its 11 places so we start out that far and work backwards.
		//this will leave a gap but that is not a problem.
		byte[] target = rb.byteBuffer;
		int tmp = value;    	
		while (tmp!=0) {
			//do not touch these 2 lines they make use of secret behavior in hot spot that does a single divide.
			int t = tmp/10;
			int r = tmp%10;
			target[rb.byteMask&--idx] = (byte)('0'+r);
			tmp = t;
		}
		target[rb.byteMask& (idx-1)] = (byte)'-';
		//to make it positive we jump over the sign.
		idx -= (1&(value>>31));
		return idx;
	}

	public static int leftConvertLongToASCII(RingBuffer rb, long value,	int idx) {
		//max places is value for -2B therefore its 11 places so we start out that far and work backwards.
		//this will leave a gap but that is not a problem.
		byte[] target = rb.byteBuffer;
		long tmp = value;    	
		while (tmp!=0) {
			//do not touch these 2 lines they make use of secret behavior in hot spot that does a single divide.
			long t = tmp/10;
			long r = tmp%10;
			target[rb.byteMask&--idx] = (byte)('0'+r);
			tmp = t;
		}
		target[rb.byteMask& (idx-1)] = (byte)'-';
		//to make it positive we jump over the sign.
		idx -= (1&(value>>63));
		return idx;
	}

	public static int readInt(int[] buffer, int mask, long index) {
		return buffer[mask & (int)(index)];
	}

	public static long readLong(int[] buffer, int mask, long index) {
		return (((long) buffer[mask & (int)index]) << 32) | (((long) buffer[mask & (int)(index + 1)]) & 0xFFFFFFFFl);
	}

	/**
	   * Convert bytes into chars using UTF-8.
	   * 
	   *  High 32   BytePosition
	   *  Low  32   Char (caller can cast response to char to get the decoded value)  
	   * 
	   */
	  public static long decodeUTF8Fast(byte[] source, long posAndChar, int mask) { //pass in long of last position?
	      //TODO: these masks appear to be wrong.
		  
		  // 7  //high bit zero all others its 1
		  // 5 6
		  // 4 6 6
		  // 3 6 6 6
		  // 2 6 6 6 6
		  // 1 6 6 6 6 6
		  
	    int sourcePos = (int)(posAndChar >> 32); 
	    
	    byte b;   
	    if ((b = source[mask&sourcePos++]) >= 0) {
	        // code point 7
	        return (((long)sourcePos)<<32) | (long)b; //1 byte result of 7 bits with high zero
	    } 
	    
	    int result;
	    if (((byte) (0xFF & (b << 2))) >= 0) {
	        if ((b & 0x40) == 0) {        	
	            ++sourcePos;
	            return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	        }
	        // code point 11
	        result = (b & 0x1F); //5 bits
	    } else {
	        if (((byte) (0xFF & (b << 3))) >= 0) {
	            // code point 16
	            result = (b & 0x0F); //4 bits
	        } else {
	            if (((byte) (0xFF & (b << 4))) >= 0) {
	                // code point 21
	                result = (b & 0x07); //3 bits
	            } else {
	                if (((byte) (0xFF & (b << 5))) >= 0) {
	                    // code point 26
	                    result = (b & 0x03); // 2 bits
	                } else {
	                    if (((byte) (0xFF & (b << 6))) >= 0) {
	                        // code point 31
	                        result = (b & 0x01); // 1 bit
	                    } else {
	                        // the high bit should never be set
	                        sourcePos += 5;
	                        return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	                    }
	
	                    if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	                        sourcePos += 5;
	                        return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	                    }
	                    result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
	                }
	                if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	                    sourcePos += 4;
	                    return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	                }
	                result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
	            }
	            if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	                sourcePos += 3;
	                return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	            }
	            result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
	        }
	        if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	            sourcePos += 2;
	            return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	        }
	        result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
	    }
	    if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	       System.err.println("Invalid encoding, low byte must have bits of 10xxxxxx but we find "+Integer.toBinaryString(source[mask&sourcePos]));
	       sourcePos += 1;
	       return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	    }
	    long chr = ((result << 6) | (int)(source[mask&sourcePos++] & 0x3F)); //6 bits
	    return (((long)sourcePos)<<32) | chr;
	  }

	public static int addASCIIToBytes(CharSequence source, int sourceIdx, int sourceLen, RingBuffer rbRingBuffer) {
		final int p = rbRingBuffer.byteWorkingHeadPos.value;
		//TODO: revisit this not sure this conditional is required
	    if (sourceLen > 0) {
	    	int targetMask = rbRingBuffer.byteMask;
	    	int proposedEnd = p + sourceLen;
			byte[] target = rbRingBuffer.byteBuffer;        	
			
	        int tStart = p & targetMask;
			if (tStart < ((p + sourceLen - 1) & targetMask)) {
				RingBuffer.copyASCIIToByte(source, sourceIdx, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    int firstLen = 1+ targetMask - tStart;
			    RingBuffer.copyASCIIToByte(source, sourceIdx, target, tStart, firstLen);
			    RingBuffer.copyASCIIToByte(source, sourceIdx + firstLen, target, 0, sourceLen - firstLen);
			}
	        rbRingBuffer.byteWorkingHeadPos.value = proposedEnd;
	    }
		return p;
	}

    public static int addASCIIToBytes(char[] source, int sourceIdx,	int sourceLen, RingBuffer rbRingBuffer) {
		final int p = rbRingBuffer.byteWorkingHeadPos.value;
	    if (sourceLen > 0) {
	    	int targetMask = rbRingBuffer.byteMask;
	    	int proposedEnd = p + sourceLen;
			byte[] target = rbRingBuffer.byteBuffer;        	
			
	        int tStop = (p + sourceLen) & targetMask;
			int tStart = p & targetMask;
			if (tStop > tStart) {
				copyASCIIToByte(source, sourceIdx, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    int firstLen = 1+ targetMask - tStart;
			    copyASCIIToByte(source, sourceIdx, target, tStart, firstLen);
			    copyASCIIToByte(source, sourceIdx + firstLen, target, 0, sourceLen - firstLen);
			}
	        rbRingBuffer.byteWorkingHeadPos.value = proposedEnd;
	    }
		return p;
	}

	public static void copyASCIIToByte(char[] source, int sourceIdx, byte[] target, int targetIdx, int len) {
		int i = len;
		while (--i>=0) {
			target[targetIdx+i] = (byte)(0xFF&source[sourceIdx+i]);
		}
	}

	public static void copyASCIIToByte(CharSequence source, int sourceIdx, byte[] target, int targetIdx, int len) {
		int i = len;
		while (--i>=0) {
			target[targetIdx+i] = (byte)(0xFF&source.charAt(sourceIdx+i));
		}
	}

	public static int copyUTF8ToByte(CharSequence source, int sourceIdx, byte[] target, int targetMask, int targetIdx, int charCount) {
	
	    int pos = targetIdx;
	    int c = 0;        
	    while (c < charCount) {
	        pos = encodeSingleChar((int) source.charAt(sourceIdx+c++), target, targetMask, pos);
	    }		
	    return pos - targetIdx;
	}

	public static int copyUTF8ToByte(char[] source, int sourceIdx, byte[] target, int targetMask, int targetIdx, int charCount) {
	
	    int pos = targetIdx;
	    int c = 0;        
	    while (c < charCount) {	    	
	        pos = encodeSingleChar((int) source[sourceIdx+c++], target, targetMask, pos);
	    }		
	    return pos - targetIdx;
	}

	public static int encodeSingleChar(int c, byte[] buffer,int mask, int pos) {
	
	    if (c <= 0x007F) {
	        // code point 7
	        buffer[mask&pos++] = (byte) c;
	    } else {
	        if (c <= 0x07FF) {
	            // code point 11
	            buffer[mask&pos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
	        } else {
	            if (c <= 0xFFFF) {
	                // code point 16
	                buffer[mask&pos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
	            } else {
	                if (c < 0x1FFFFF) {
	                    // code point 21
	                    buffer[mask&pos++] = (byte) (0xF0 | ((c >> 18) & 0x07));
	                } else {
	                    if (c < 0x3FFFFFF) {
	                        // code point 26
	                        buffer[mask&pos++] = (byte) (0xF8 | ((c >> 24) & 0x03));
	                    } else {
	                        if (c < 0x7FFFFFFF) {
	                            // code point 31
	                            buffer[mask&pos++] = (byte) (0xFC | ((c >> 30) & 0x01));
	                        } else {
	                            throw new UnsupportedOperationException("can not encode char with value: " + c);
	                        }
	                        buffer[mask&pos++] = (byte) (0x80 | ((c >> 24) & 0x3F));
	                    }
	                    buffer[mask&pos++] = (byte) (0x80 | ((c >> 18) & 0x3F));
	                }
	                buffer[mask&pos++] = (byte) (0x80 | ((c >> 12) & 0x3F));
	            }
	            buffer[mask&pos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
	        }
	        buffer[mask&pos++] = (byte) (0x80 | (c & 0x3F));	        
	    }
	
	    return pos;
	}

	public static void addByteBuffer(RingBuffer rb, ByteBuffer source, int length) {
		validateVarLength(rb, length);
		int bytePos = rb.byteWorkingHeadPos.value;    
		int partialLength = 1 + rb.byteMask - (bytePos & rb.byteMask);    		
		if (partialLength<length) {   		
			//read from source and write into byteBuffer
			source.get(rb.byteBuffer, bytePos & rb.byteMask, partialLength);
			source.get(rb.byteBuffer, 0, length - partialLength);					    		
		} else {					    	
			source.get(rb.byteBuffer, bytePos & rb.byteMask, length);
		}
		rb.byteWorkingHeadPos.value = bytePos + length;
	}

	public static void addByteArrayWithMask(final RingBuffer outputRing, int mask, int len, byte[] data, int offset) {
		if ((offset&mask) <= ((offset+len-1) & mask)) {
			
			//simple add bytes
			addByteArray(data, offset&mask, len, outputRing);
			 
		} else {						
			
			//rolled over the end of the buffer
			int len1 = 1+mask-(offset&mask);
			appendPartialBytesArray(data, offset&mask, len1, outputRing.byteBuffer, outputRing.byteWorkingHeadPos.value, outputRing.byteMask);        
			appendPartialBytesArray(data, 0, len-len1, outputRing.byteBuffer, outputRing.byteWorkingHeadPos.value, outputRing.byteMask);        
			
			addBytePosAndLen(outputRing.buffer, outputRing.mask, outputRing.workingHeadPos, outputRing.bytesHeadPos.get(), outputRing.byteWorkingHeadPos.value, len);
			outputRing.byteWorkingHeadPos.value = outputRing.byteWorkingHeadPos.value + len;
		}
	}

	public static int peek(int[] buf, long pos, int mask) {
        return buf[mask & (int)pos];
    }

    public static long peekLong(int[] buf, long pos, int mask) {
        
        return (((long) buf[mask & (int)pos]) << 32) | (((long) buf[mask & (int)(pos + 1)]) & 0xFFFFFFFFl);

    }
    
    public static boolean isShutDown(RingBuffer ring) {
    	return ring.shutDown.get();
    }
    
    public static void shutDown(RingBuffer ring) {
    	ring.shutDown.set(true);
    }    

    public static void addByteArray(byte[] source, int sourceIdx, int sourceLen, RingBuffer rbRingBuffer) {
    	
    	assert(sourceLen>=0);
    	validateVarLength(rbRingBuffer, sourceLen);
    	
    	appendPartialBytesArray(source, sourceIdx, sourceLen, rbRingBuffer.byteBuffer, rbRingBuffer.byteWorkingHeadPos.value, rbRingBuffer.byteMask);   
        addBytePosAndLen(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, rbRingBuffer.bytesHeadPos.get(), rbRingBuffer.byteWorkingHeadPos.value, sourceLen);
        rbRingBuffer.byteWorkingHeadPos.value = rbRingBuffer.byteWorkingHeadPos.value + sourceLen;		
		
    }
    
    public static void addNullByteArray(RingBuffer rbRingBuffer) {
        addBytePosAndLen(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, rbRingBuffer.bytesHeadPos.get(), rbRingBuffer.byteWorkingHeadPos.value, -1);
    }
    

	public static void appendPartialBytesArray(byte[] source, int sourceIdx, int sourceLen,
			                                   byte[] target, final int targetBytePos, int targetMask) {
		appendPartialBytesArray2(source, sourceIdx, sourceLen, target, (targetBytePos + sourceLen) & targetMask, targetBytePos & targetMask);
	}

	private static void appendPartialBytesArray2(byte[] source, int sourceIdx,
			int sourceLen, byte[] target, int tStop, int tStart) {
		if (tStop >= tStart) {
		    System.arraycopy(source, sourceIdx, target, tStart, sourceLen);
		} else {
			// done as two copies
		    System.arraycopy(source, sourceIdx, target, tStart, sourceLen - tStop);
		    System.arraycopy(source, sourceIdx + sourceLen - tStop, target, 0, tStop);
		}
	}
    
    public static void addValue(RingBuffer rb, int value) {
		 addValue(rb.buffer, rb.mask, rb.workingHeadPos, value);		
	}
 
    //must be called by low-level API when starting a new message
    public static void addMsgIdx(RingBuffer rb, int value) {
    	
    	 assert(rb.consumerData.nextWorkingHead<=rb.headPos.get() || rb.workingHeadPos.value<=rb.consumerData.nextWorkingHead) : "Unsupported mix of high and low level API.";
    	   
    	 rb.bytesHeadPos.lazySet(rb.byteWorkingHeadPos.value);    	
		 addValue(rb.buffer, rb.mask, rb.workingHeadPos, value);		
	}
    
   
    //we are only allowed 12% of the time or so for doing this write.
    //this pushes only ~5gbs but if we had 100% it would scale to 45gbs
    //so this is not the real bottleneck and given the compression ratio of the test data
    //we can push 1gbs more of compressed data for each 10% of cpu freed up.
    public static void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value) {
        buffer[rbMask & (int)headCache.value++] = value;
    } 
    
    public static void setValue(int[] buffer, int rbMask, long offset, int value) {
        buffer[rbMask & (int)offset] = value;
    } 
    
    public static void addBytePosAndLen(int[] buffer, int rbMask, PaddedLong headCache, int baseBytePos, int position, int length) {
    	long p = headCache.value; 
        setBytePosAndLen(buffer, rbMask, p, position, length, baseBytePos);        
        headCache.value = p+2;
        
    }

    //TODO: AAA, add tail step size for the high level API fixed offsets. (abs position in the ring buffer? not len)
    
	public static void setBytePosAndLen(int[] buffer, int rbMask, long ringPos,	int positionDat, int lengthDat, int bytesHeadPos) {
	   	//negative position is written as is because the internal array does not have any offset (but it could some day)
    	//positive position is written after subtracting the rbRingBuffer.bytesHeadPos.longValue()
    	int tmp = positionDat;
    	if (positionDat>=0) {
    		tmp = (int)(positionDat-bytesHeadPos);
    		assert(tmp>=0);
    	}
    	
        buffer[rbMask & (int)ringPos] = tmp;
        buffer[rbMask & (int)(ringPos+1)] = lengthDat;
	} 
    
	public static int restorePosition(RingBuffer ring, int pos) {
		assert(pos>=0);
		return pos+ring.bytesTailPos.get();
	}

    public static int bytePosition(int meta, RingBuffer ring, int len) {
    	    	    	
    	//NOTE: must move this working position for the relative text positions until it gets managed by high level API.
        if (len>=0) {
        	ring.byteWorkingTailPos.value += len;
        }
        return restorePosition(ring, meta & 0x7FFFFFFF);
    }   
	
    public static void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value1, int value2, int value3) {
        
        long p = headCache.value; 
        buffer[rbMask & (int)p++] = value1;
        buffer[rbMask & (int)p++] = value2;
        buffer[rbMask & (int)p++] = value3;
        headCache.value = p;
        
    }    
    
    public static void addValues(int[] buffer, int rbMask, PaddedLong headCache, int value1, long value2) {
        
        headCache.value = setValues(buffer, rbMask, headCache.value, value1, value2);
        
    }

	public static long setValues(int[] buffer, int rbMask, long pos,
			int value1, long value2) {
		buffer[rbMask & (int)pos++] = value1;
        buffer[rbMask & (int)pos++] = (int)(value2 >>> 32);
        buffer[rbMask & (int)pos++] = (int)(value2 & 0xFFFFFFFF);
		return pos;
	}   
    
    public static void addLongValue(int[] buffer, int rbMask, PaddedLong headCache, long value) {
        
        long p = headCache.value; 
        buffer[rbMask & (int)p] = (int)(value >>> 32);
        buffer[rbMask & (int)(p+1)] = (int)(value & 0xFFFFFFFF);
        headCache.value = p+2;
        
    } 
    
    public static void dump(RingBuffer rb) {
                       
        // move the removePosition up to the addPosition
        // new Exception("WARNING THIS IS NO LONGER COMPATIBLE WITH PUMP CALLS").printStackTrace();
        rb.tailPos.lazySet(rb.workingTailPos.value = rb.workingHeadPos.value);
    }

    // WARNING: consumer of these may need to loop around end of buffer !!
    // these are needed for fast direct READ FROM here

    public static int readRingByteLen(int fieldPos, int[] rbB, int rbMask, PaddedLong rbPos) {
        return rbB[(int) (rbMask & (rbPos.value + fieldPos + 1))];// second int is always the length
    }

	public static int readRingByteLen(int idx, RingBuffer ring) {
		return readRingByteLen(idx,ring.buffer,ring.mask,ring.workingTailPos);       
	}
	
	public static int takeRingByteLen(RingBuffer ring) {		
		return ring.buffer[(int)(ring.mask & (ring.workingTailPos.value++))];// second int is always the length     
	}
    
 

    public static byte[] byteBackingArray(int meta, RingBuffer rbRingBuffer) {
        return rbRingBuffer.bufferLookup[meta>>>31];
    }
    
	public static int readRingByteMetaData(int pos, RingBuffer rb) {
		return readValue(pos,rb.buffer,rb.mask,rb.workingTailPos.value);
	}
			
	public static int takeRingByteMetaData(RingBuffer ring) {
		return readValue(0,ring.buffer,ring.mask,ring.workingTailPos.value++);
	}
	
    public static int readValue(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int)(rbMask & (rbPos + fieldPos))];
    }
   
    public static int readValue(int idx, RingBuffer ring) {    	
    	return readValue(idx, ring.buffer,ring.mask,ring.workingTailPos.value);
    }
    
    public static int takeValue(RingBuffer ring) {    	
    	return readValue(0, ring.buffer,ring.mask,ring.workingTailPos.value++);
    }
    
    public static int contentRemaining(RingBuffer rb) {
        return (int)(rb.headPos.longValue() - rb.tailPos.longValue()); //must not go past add count because it is not release yet.
    }

    public static void setWorkingTailPosition(RingBuffer ring, long position) {
    	ring.workingTailPos.value = position;
    }
    
    public static long getWorkingTailPosition(RingBuffer ring) {
    	return ring.workingTailPos.value;
    }
    
    public static void releaseReadLock(RingBuffer ring) {
    	ring.tailPos.lazySet(ring.workingTailPos.value);
    	ring.bytesTailPos.lazySet(ring.byteWorkingTailPos.value);
    }
    
    public static void publishWrites(RingBuffer ring) {
    	
    	if (ring.writeTrailingCountOfBytesConsumed) {
			writeTrailingCountOfBytesConsumed(ring, ring.workingHeadPos.value++); //increment because this is the low-level API calling
		} //MUST be before the assert.
    	
    	assert(ring.consumerData.nextWorkingHead<=ring.headPos.get() || ring.workingHeadPos.value<=ring.consumerData.nextWorkingHead) : "Unsupported mix of high and low level API.";
    	    	
    	//publish this first so the bulk splitter will pick up all the values
    	ring.bytesHeadPos.lazySet(ring.byteWorkingHeadPos.value);
    	//publish writes
    	ring.headPos.lazySet(ring.workingHeadPos.value);
    }
    
    public static void abandonWrites(RingBuffer ring) {    
        //ignore the fact that any of this was written to the ring buffer
    	ring.workingHeadPos.value = ring.headPos.longValue();
    	ring.byteWorkingHeadPos.value = ring.bytesHeadPos.intValue();
    }


    //All the spin lock methods share the same implementation. Unfortunately these can not call 
    //a common implementation because the extra method jump degrades the performance in tight loops
    //where these spin locks are commonly used.
    
    public static long spinBlockOnTailTillMatchesHead(long lastCheckedValue, RingBuffer ringBuffer) {
    	long targetValue = ringBuffer.headPos.longValue();
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield(); //needed for now but re-evaluate performance impact
    		if (isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
    			throw new RingBufferException("Unexpected shutdown");
    		}
		    lastCheckedValue = ringBuffer.tailPos.longValue();
		} 
		return lastCheckedValue;
    }
    
    public static long spinBlockOnTail(long lastCheckedValue, long targetValue, RingBuffer ringBuffer) {
    	
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield();//needed for now but re-evaluate performance impact
    		if (isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
    			throw new RingBufferException("Unexpected shutdown");
    		}
		    lastCheckedValue = ringBuffer.tailPos.longValue();
		}
		return lastCheckedValue;
    }
    
    public static long spinBlockOnHeadTillMatchesTail(long lastCheckedValue, RingBuffer ringBuffer) {
    	long targetValue = ringBuffer.tailPos.longValue();    	
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield();//needed for now but re-evaluate performance impact
    		if (isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
    			throw new RingBufferException("Unexpected shutdown");
    		}
		    lastCheckedValue = ringBuffer.headPos.longValue();
		}
		return lastCheckedValue;
    }
    
    public static long spinBlockOnHead(long lastCheckedValue, long targetValue, RingBuffer ringBuffer) {
    	
    	//we are blocking before we can read
    	
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield();//needed for now but re-evaluate performance impact
    		if (isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
    			throw new RingBufferException("Unexpected shutdown");
    		}
		    lastCheckedValue = ringBuffer.headPos.longValue();
		}
		return lastCheckedValue;
    }
    
	public static int byteMask(RingBuffer ring) {
		return ring.byteMask;
	}

	public static long headPosition(RingBuffer ring) {
		 return ring.headPos.get();
	}

	public static long tailPosition(RingBuffer ring) {
		return ring.tailPos.get();
	}

	public static int primarySize(RingBuffer ring) {
		return ring.maxSize;
	}

	public static FieldReferenceOffsetManager from(RingBuffer ring) {
		return ring.consumerData.from;
	}

	public static void writeTrailingCountOfBytesConsumed(RingBuffer ring, long pos) {
		ring.buffer[ring.mask & (int)pos] = ring.byteWorkingHeadPos.value - ring.bytesHeadPos.get();			
		ring.writeTrailingCountOfBytesConsumed = false;
	}
	
}
