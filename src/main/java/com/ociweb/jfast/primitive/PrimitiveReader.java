//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import java.io.IOException;

import com.ociweb.jfast.error.FASTException;

/**
 * PrimitiveReader
 * 
 * Must be final and not implement any interface or be abstract.
 * In-lining the primitive methods of this class provides much
 * of the performance needed by this library.
 * 
 * 
 * @author Nathan Tippy
 *
 */

public final class PrimitiveReader {

	// Runs very well with these JVM arguments
	// -XX:CompileThreshold=8 -XX:+AlwaysPreTouch -XX:+UseNUMA -XX:MaxInlineLevel=12 -XX:InlineSmallCode=16300
			
	//Note: only the type/opp combos used will get in-lined, this small footprint will fit in execution cache.
	//      if we in-line too much the block will be to large and may spill.
	
	
	private final FASTInput input;
	private long totalReader;
	final byte[] buffer;
	
	private final byte[] invPmapStack;
	private int invPmapStackDepth;

	private int position;
	private int limit;
	
	//both bytes but class def likes int much better for alignment
	private byte pmapIdx = -1; 
	private byte bitBlock = 0;
	
	
	public void reset() {
		totalReader = 0;
		position = 0;
		limit = 0;
		pmapIdx = -1;
		invPmapStackDepth = invPmapStack.length-2;
		
	}

	//TODO: add primitive methods for skipping fields. with Unit tests

	public PrimitiveReader(FASTInput input) {
		this(2048,input,32);
	}
	
	public PrimitiveReader(int initBufferSize, FASTInput input, int maxPMapCount) {
		this.input = input;
		this.buffer = new byte[initBufferSize];
		
		this.position = 0;
		this.limit = 0;
		this.invPmapStack = new byte[maxPMapCount]; //TODO: need two bytes of gap between each!!! how to external?
		this.invPmapStackDepth = invPmapStack.length-2;
				
		input.init(this.buffer);
	}
	
	public long totalRead() {
		return totalReader;
	}
	public int bytesReadyToParse() {
		return limit-position;
	}
	
	public void fetch() {
		fetch(0);
	}
	
	//Will not return until the need is met because the parser has
	//determined that we can not continue until this data is provided.
	//this call may however read in more than the need because its ready
	//and convenient to reduce future calls.
	private void fetch(int need) {
		int count = 0;
		need = fetchAvail(need);
		while (need>0) { //TODO: if orignial need was zero should also compact?
			if (0==count++) {
				
				//compact and prep for data spike
				
			} else {
				if (count<10) { 
					Thread.yield();
					//TODO: if we are in the middle of parsing a field this becomes a blocking read and requires a timeout and throw.
					
				} else {				
					try {
						Thread.sleep(0, 100);
					} catch (InterruptedException e) {
					}						
				}
			}
			
			
			need = fetchAvail(need);
		}
				
	}
	
	private int fetchAvail(int need) {
		if (position >= limit) {
			position = limit = 0;
		}
		int remainingSpace = buffer.length-limit;
		if (need <= remainingSpace) {	
			//fill remaining space if possible to reduce fetch later
						
			int filled = input.fill(limit, remainingSpace);

			//
			totalReader += filled;
			limit += filled;			
			//
			return need-filled;
		} else {
			return noRoomOnFetch(need);
		}
	}

	private int noRoomOnFetch(int need) {
		//not enough room at end of buffer for the need
		int populated = limit - position;
		int reqiredSize = need + populated;
		
		assert(buffer.length>=reqiredSize) : "internal buffer is not large enough, requres "+reqiredSize+" bytes";
		
		System.arraycopy(buffer, position, buffer, 0, populated);
		//fill and return
		
		int filled = input.fill(populated, buffer.length - populated);
		
		
		position = 0;
		totalReader+=filled;
		limit = populated+filled;
			
		return need-filled;
		
	}
		
	public final void readByteData(byte[] target, int offset, int length) {
		//ensure all the bytes are in the buffer before calling visitor
        if (limit - position < length) {
			fetch(length);
		}
		System.arraycopy(buffer, position, target, offset, length);
		position+=length;
	}
		
    /////////////////
	//pmapStructure
	//          1 2 3 4 5 D ? I 2 3 4 X X
	//          0 0 0 0 1 D ? I 0 0 1 X X
	//
	// D delta to last position
	// I pmapIdx of last stack frame	
	////
	//called at the start of each group unless group knows it has no pmap
	public final void openPMap(final int pmapMaxSize) {
		//push the old index for resume
		invPmapStack[invPmapStackDepth-1] = (byte)pmapIdx;

		int k = invPmapStackDepth -= (pmapMaxSize+2); 
    	if (position>=limit) {
			fetch(1);
		}		
		bitBlock = buffer[position];
		if (limit-position>pmapMaxSize) {
	        do {
	        	//System.err.println("*pmap:"+Integer.toBinaryString(0xFF&buffer[position]));
			} while ((invPmapStack[k++] = buffer[position++])>=0);	
		} else {
			//must use slow path because we are near the end of the buffer.
	        do {				
	        	if (position>=limit) {
					fetch(1);
				}				
	        	//System.err.println("*pmap:"+Integer.toBinaryString(0xFF&buffer[position]));
			} while ((invPmapStack[k++] = buffer[position++])>=0);
		}
        invPmapStack[k] = (byte)(3+pmapMaxSize+(invPmapStackDepth-k));
		
        //set next bit to read
        pmapIdx = 6;
	}
	
	//called at every field to determine operation
	public final byte popPMapBit() {		
			byte tmp = pmapIdx;
			byte bb = bitBlock;
			if (tmp>0 || (tmp==0 && bb<0)) {
				//Frequent, 6 out of every 7 plus the last bit block
				pmapIdx = (byte)(tmp-1);	
				return (byte)(1&(bb>>>tmp));

			} else {
				return popPMapBitLow(tmp, bb);
			}

	}


	private byte popPMapBitLow(byte tmp, byte bb) {
		if (tmp>=0) {	
			//SOMETIMES one of 7 we need to move up to the next byte
			//System.err.println(invPmapStackDepth);
			//The order of these lines should not be changed without profile
			pmapIdx = 6;
			byte result = (byte)(1&bb);					
			bitBlock = invPmapStack[++invPmapStackDepth];
			return result;

		} else {
			return 0;
		}
	}

	//called at the end of each group
	public final void closePMap() {
		//assert(bitBlock<0);
		assert(invPmapStack[invPmapStackDepth+1]>=0);
		bitBlock = invPmapStack[invPmapStackDepth += (invPmapStack[invPmapStackDepth+1])];
		pmapIdx = invPmapStack[invPmapStackDepth-1];

	}
	
	/////////////////////////////////////
	/////////////////////////////////////
	/////////////////////////////////////
	
	public final long readLongSigned () {
		if (limit-position<=10) {
			return readLongSignedSlow();
		}
		
		long v = buffer[position++];						
		long accumulator = ((v&0x40)==0) ? 0l :0xFFFFFFFFFFFFFF80l;
 
		while (v>=0) { 
			accumulator = (accumulator|v)<<7;
			v = buffer[position++];
		}
    
	    return accumulator|(v&0x7Fl);
	}


	private long readLongSignedSlow() {
		//slow path
		if (position>=limit) {
			fetch(1);
		}
		int v = buffer[position++];
		long accumulator = ((v&0x40)==0) ? 0 :0xFFFFFFFFFFFFFF80l;

		while (v>=0) { //(v & 0x80)==0) {
			if (position>=limit) {
				fetch(1);
			}
			accumulator = (accumulator|v)<<7;
			v = buffer[position++];
		}
		return accumulator|(v&0x7F);
	}
	

	public final long readLongUnsigned () {
		if (position>limit-10) {
			if (position>=limit) {
				fetch(1);
			}
			byte v = buffer[position++];
			long accumulator;
			if (v>=0) { //(v & 0x80)==0) {
				accumulator = v<<7;
			} else {
				return (v&0x7F);
			}
			
			if (position>=limit) {
				fetch(1);
			}
			v = buffer[position++];
			
		    while (v>=0) { //(v & 0x80)==0) {
		    	accumulator = (accumulator|v)<<7;
		    	
		    	if (position>=limit) {
					fetch(1);
				}
		    	v = buffer[position++];
		    	
		    }
		    return accumulator|(v&0x7F);
		}
		byte[] buf = buffer;

		byte v = buf[position++];
		long accumulator;
		if (v>=0) {//(v & 0x80)==0) {
			accumulator = v<<7;
		} else {
			return (v&0x7F);
		}
		
		v = buf[position++];
	    while (v>=0) {//(v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buf[position++];
	    }
	    return accumulator|(v&0x7F);
	}
	
	public final int readIntegerSigned () {
		if (limit-position<=5) {
			return readIntegerSignedSlow();
		}
		int p = position;
		byte v = buffer[p++];
		int accumulator = ((v&0x40)==0) ? 0 :0xFFFFFF80;

	    while (v>=0) {  //(v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buffer[p++];
	    }
	    position = p;
	    return accumulator|(v&0x7F);
	}

	private int readIntegerSignedSlow() {
		if (position>=limit) {
			fetch(1);
		}
		byte v = buffer[position++];
		int accumulator = ((v&0x40)==0) ? 0 :0xFFFFFF80;

		while (v>=0) {  //(v & 0x80)==0) {
			if (position>=limit) {
				fetch(1);
			}
			accumulator = (accumulator|v)<<7;
			v = buffer[position++];
		}
		return accumulator|(v&0x7F);
	}
	
	
	public final int readIntegerUnsigned() {
		
		if (limit-position>=5) {//not near end so go fast.
	
			byte v = buffer[position++];
			int accumulator;
			if (v<0) {
				return (v&0x7F);
			} else {
				accumulator = v<<7;
			}
			
			v = buffer[position++];
			if (v<0) {
				return accumulator|(v&0x7F);
			} else {
				accumulator = (accumulator|v)<<7;
			}			
			
		    while ((v = buffer[position++])>=0) { //(v & 0x80)==0) {
		    	accumulator = (accumulator|v)<<7;
		    }
		    return accumulator|(v&0x7F);
	   } else { 
		   return readIntegerUnsignedSlow();
	   }
	}

	private int readIntegerUnsignedSlow() {
		if (position>=limit) {
			fetch(1);
		}
		byte v = buffer[position++];
		int accumulator;
		if (v>=0) { //(v & 0x80)==0) {
			accumulator = v<<7;
		} else {
			return (v&0x7F);
		}
		
		if (position>=limit) {
			fetch(1);
		}
		v = buffer[position++];

		while (v>=0) { //(v & 0x80)==0) {
			accumulator = (accumulator|v)<<7;
			if (position>=limit) {
				fetch(1);
			}
			v = buffer[position++];
		}
		return accumulator|(v&0x7F);
	}

	public Appendable readTextASCII(Appendable target) {
		if (limit - position < 2) {
			fetch(2);
		}
		
		byte v = buffer[position];
		
		if (0 == v) {
			v = buffer[position+1];
			if (0x80 != (v&0xFF)) {
				throw new UnsupportedOperationException();
			}
			//nothing to change in the target
			position+=2;
		} else {	
			//must use count because the base of position will be in motion.
			//however the position can not be incremented or fetch may drop data.

			while (buffer[position]>=0) {
				try {
					target.append((char)(buffer[position]));
				} catch (IOException e) {
					throw new FASTException(e);
				}
				position++;
				if (position>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
			}
			try {
				target.append((char)(0x7F & buffer[position]));
			} catch (IOException e) {
				throw new FASTException(e);
			}
			
			position++;				
			
		}
		return target;
	}

	public int readTextASCII(char[] target, int targetOffset, int targetLimit) {
		
		
		
		//TODO:add fast copy by fetch of limit
		//then return error when limit is reached? Do not call fetch on limit we do not know that we need them.
		
		
		if (limit - position < 2) {
			fetch(2);
		}
		
		byte v = buffer[position];
				
		if (0 == v) {
			v = buffer[position+1];
			if (0x80 != (v&0xFF)) {
				throw new UnsupportedOperationException();
			}
			//nothing to change in the target
			position+=2;
			return 0; //zero length string
		} else {	
			int countDown = targetLimit-targetOffset;
			//must use count because the base of position will be in motion.
			//however the position can not be incremented or fetch may drop data.
            int idx = targetOffset;
			while (buffer[position]>=0 && --countDown>=0) {
				target[idx++]=(char)(buffer[position++]);
				if (position>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
			}
			if (--countDown>=0) {
				target[idx++]=(char)(0x7F & buffer[position++]);
				return idx-targetOffset;//length of string
			} else {
				return targetOffset-idx;//neg length of string if hit max
			}
		}
	}
	
	public int readTextASCII2(char[] target, int targetOffset, int targetLimit) {
		
		int countDown = targetLimit-targetOffset;
		if (limit - position >= countDown) {
			//System.err.println("fast");
			//must use count because the base of position will be in motion.
			//however the position can not be incremented or fetch may drop data.
	        int idx = targetOffset;
			while (buffer[position]>=0 && --countDown>=0) {
				target[idx++]=(char)(buffer[position++]);
			}
			if (--countDown>=0) {
				target[idx++]=(char)(0x7F & buffer[position++]);
				return idx-targetOffset;//length of string
			} else {
				return targetOffset-idx;//neg length of string if hit max
			}
		} else {
			return readAsciiText2Slow(target, targetOffset, countDown);
		}
	}

	private int readAsciiText2Slow(char[] target, int targetOffset, int countDown) {
		if (limit - position < 2) {
			fetch(2);
		}
		
		//must use count because the base of position will be in motion.
		//however the position can not be incremented or fetch may drop data.
		int idx = targetOffset;
		while (buffer[position]>=0 && --countDown>=0) {
			target[idx++]=(char)(buffer[position++]);
			if (position>=limit) {
				fetch(1); //CAUTION: may change value of position
			}
		}
		if (--countDown>=0) {
			target[idx++]=(char)(0x7F & buffer[position++]);
			return idx-targetOffset;//length of string
		} else {
			return targetOffset-idx;//neg length of string if hit max
		}
	}

	//keep calling while byte is >=0
	public byte readTextASCIIByte() {
		if (position>=limit) {
			fetch(1); //CAUTION: may change value of position
		}
		return buffer[position++];
	}
	
	public Appendable readTextUTF8(int charCount, Appendable target) {

		while (--charCount>=0) {
			if (position>=limit) {
				fetch(1); //CAUTION: may change value of position
			}
			byte b = buffer[position++];
			if (b>=0) {
				//code point 7
				try {
					target.append((char)b);
				} catch (IOException e) {
					throw new FASTException(e);
				}
			} else {
				decodeUTF8(target, b);
			}
		}
		return target;
	}
	
	public void readSkipByStop() {
		if (position>=limit) {
			fetch(1);
		}
		while (buffer[position++]>=0) { 
			if (position>=limit) {
				fetch(1);
			}
		}
	}
	
	public void readSkipByLengthByt(int len) {
		if (limit - position < len) {
			fetch(len);
		}
		position+=len;
	}
	
	public void readSkipByLengthUTF(int len) {
		//len is units of utf-8 chars so we must check the
		//code points for each before fetching and jumping.
		//no validation at all because we are not building a string.
		while (--len>=0) {
			if (position>=limit) {
				fetch(1);
			}
			byte b = buffer[position++];
			if (b<0) {
				//longer pattern than 1 byte
				if (0!=(b&0x20)) {
					//longer pattern than 2 bytes
					if (0!=(b&0x10)) {
						//longer pattern than 3 bytes
						if (0!=(b&0x08)) {
							//longer pattern than 4 bytes
							if (0!=(b&0x04)) {
								//longer pattern than 5 bytes
								if (position>=limit) {
									fetch(5);
								}
								position+=5;
							} else {
								if (position>=limit) {
									fetch(4);
								}	
								position+=4;
							}
						} else {
							if (position>=limit) {
								fetch(3);
							}
							position+=3;
						}	
					} else {
						if (position>=limit) {
							fetch(2);
						}
						position+=2;
					}
				} else {
					if (position>=limit) {
						fetch(1);
					}
					position++;
				}								
			}			
		}
	}
	
	
	
	public void readTextUTF8(char[] target, int offset, int charCount) {
		//System.err.println("B");
		byte b;
		if (limit-position >= charCount<<3) { //if bigger than the text could be then use this shortcut
			//fast
			while (--charCount>=0) {
				if ((b = buffer[position++])>=0) {
					//code point 7
					target[offset++] = (char)b;
				} else {
					decodeUTF8Fast(target, offset++, b);//untested?? why
				}
			}
		} else {		
			while (--charCount>=0) {
				if (position>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
				if ((b = buffer[position++])>=0) {
					//code point 7
					target[offset++] = (char)b;
				} else {
					decodeUTF8(target, offset++, b);
				}
			}
		}
	}
	
	//convert single char that is not the simple case
		private void decodeUTF8(Appendable target, byte b) {	
			byte[] source = buffer;
			
		    int result;
			if ( ((byte)(0xFF&(b<<2))) >=0) {
				if ((b&0x40)==0) {
					try {
						target.append((char)0xFFFD); //Bad data replacement char
					} catch (IOException e) {
						throw new FASTException(e);
					}
					if (position>=limit) {
						fetch(1); //CAUTION: may change value of position
					}
					++position;
					return; 
				}
				//code point 11	
				result  = (b&0x1F);	
			} else {
				if (((byte)(0xFF&(b<<3)))>=0) { //TODO: these would be faster/simpler by factoring out the constant in this comparison.
					//code point 16
					result = (b&0x0F);
				}  else {
					if (((byte)(0xFF&(b<<4)))>=0) {
						//code point 21
						result = (b&0x07);
					} else {
						if (((byte)(0xFF&(b<<5)))>=0) {
							//code point 26
							result = (b&0x03);
						} else {
							if (((byte)(0xFF&(b<<6)))>=0) {
								//code point 31
								result = (b&0x01);
							} else {
								//System.err.println("odd byte :"+Integer.toBinaryString(b)+" at pos "+(offset-1));
								//the high bit should never be set
								try{
									target.append((char)0xFFFD); //Bad data replacement char
								} catch (IOException e) {
									throw new FASTException(e);
								}
								if (limit - position < 5) {
									fetch(5);
								}
								position+=5; 
								return; 
							}
							
							if ((source[position]&0xC0)!=0x80) {
								try {
									target.append((char)0xFFFD); //Bad data replacement char
								} catch (IOException e) {
									throw new FASTException(e);
								}
								if (limit - position < 5) {
									fetch(5);
								}
								position+=5; 
								return; 
							}
							if (position>=limit) {
								fetch(1); //CAUTION: may change value of position
							}
							result = (result<<6)|(source[position++]&0x3F);
						}						
						if ((source[position]&0xC0)!=0x80) {
							try{
								target.append((char)0xFFFD); //Bad data replacement char
							} catch (IOException e) {
								throw new FASTException(e);
							}
							if (limit - position < 4) {
								fetch(4);
							}
							position+=4; 
							return; 
						}
						if (position>=limit) {
							fetch(1); //CAUTION: may change value of position
						}
						result = (result<<6)|(source[position++]&0x3F);
					}
					if ((source[position]&0xC0)!=0x80) {
						try {
							target.append((char)0xFFFD); //Bad data replacement char
						} catch (IOException e) {
							throw new FASTException(e);
						}
						if (limit - position < 3) {
							fetch(3);
						}
						position+=3; 
						return; 
					}
					if (position>=limit) {
						fetch(1); //CAUTION: may change value of position
					}
					result = (result<<6)|(source[position++]&0x3F);
				}
				if ((source[position]&0xC0)!=0x80) {
					try {
						target.append((char)0xFFFD); //Bad data replacement char
					} catch (IOException e) {
						throw new FASTException(e);
					}
					if (limit - position < 2) {
						fetch(2);
					}
					position+=2;
					return; 
				}
				if (position>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
				result = (result<<6)|(source[position++]&0x3F);
			}
			if ((source[position]&0xC0)!=0x80) {
				try {
					target.append((char)0xFFFD); //Bad data replacement char
				} catch (IOException e) {
					throw new FASTException(e);
				}
				if (position>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
				position+=1;
				return; 
			}
			try {
				if (position>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
				target.append((char)((result<<6)|(source[position++]&0x3F)));
			} catch (IOException e) {
				throw new FASTException(e);
			}
		}
	
	//convert single char that is not the simple case
	private void decodeUTF8(char[] target, int targetIdx, byte b) {
		
		byte[] source = buffer;
		
	    int result;
		if ( ((byte)(0xFF&(b<<2))) >=0) {
			if ((b&0x40)==0) {
				target[targetIdx] = 0xFFFD; //Bad data replacement char
				if (position>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
				++position;
				return; 
			}
			//code point 11	
			result  = (b&0x1F);	
		} else {
			if (((byte)(0xFF&(b<<3)))>=0) {
				//code point 16
				result = (b&0x0F);
			}  else {
				if (((byte)(0xFF&(b<<4)))>=0) {
					//code point 21
					result = (b&0x07);
				} else {
					if (((byte)(0xFF&(b<<5)))>=0) {
						//code point 26
						result = (b&0x03);
					} else {
						if (((byte)(0xFF&(b<<6)))>=0) {
							//code point 31
							result = (b&0x01);
						} else {
							//System.err.println("odd byte :"+Integer.toBinaryString(b)+" at pos "+(offset-1));
							//the high bit should never be set
							target[targetIdx] = 0xFFFD; //Bad data replacement char
							if (limit - position < 5) {
								fetch(5);
							}
							position+=5; 
							return; 
						}
						
						if ((source[position]&0xC0)!=0x80) {
							target[targetIdx] = 0xFFFD; //Bad data replacement char
							if (limit - position < 5) {
								fetch(5);
							}
							position+=5; 
							return; 
						}
						if (position>=limit) {
							fetch(1); //CAUTION: may change value of position
						}
						result = (result<<6)|(source[position++]&0x3F);
					}						
					if ((source[position]&0xC0)!=0x80) {
						target[targetIdx] = 0xFFFD; //Bad data replacement char
						if (limit - position < 4) {
							fetch(4);
						}
						position+=4; 
						return; 
					}
					if (position>=limit) {
						fetch(1); //CAUTION: may change value of position
					}
					result = (result<<6)|(source[position++]&0x3F);
				}
				if ((source[position]&0xC0)!=0x80) {
					target[targetIdx] = 0xFFFD; //Bad data replacement char
					if (limit - position < 3) {
						fetch(3);
					}
					position+=3; 
					return; 
				}
				if (position>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
				result = (result<<6)|(source[position++]&0x3F);
			}
			if ((source[position]&0xC0)!=0x80) {
				target[targetIdx] = 0xFFFD; //Bad data replacement char
				if (limit - position < 2) {
					fetch(2);
				}
				position+=2;
				return; 
			}
			if (position>=limit) {
				fetch(1); //CAUTION: may change value of position
			}
			result = (result<<6)|(source[position++]&0x3F);
		}
		if ((source[position]&0xC0)!=0x80) {
			target[targetIdx] = 0xFFFD; //Bad data replacement char
			if (position>=limit) {
				fetch(1); //CAUTION: may change value of position
			}
			position+=1;
			return; 
		}
		if (position>=limit) {
			fetch(1); //CAUTION: may change value of position
		}
		target[targetIdx] = (char)((result<<6)|(source[position++]&0x3F));
	}

	private void decodeUTF8Fast(char[] target, int targetIdx, byte b) {
		
		byte[] source = buffer;
		
	    int result;
		if ( ((byte)(0xFF&(b<<2))) >=0) {
			if ((b&0x40)==0) {
				target[targetIdx] = 0xFFFD; //Bad data replacement char
				++position;
				return; 
			}
			//code point 11	
			result  = (b&0x1F);	
		} else {
			if (((byte)(0xFF&(b<<3)))>=0) {
				//code point 16
				result = (b&0x0F);
			}  else {
				if (((byte)(0xFF&(b<<4)))>=0) {
					//code point 21
					result = (b&0x07);
				} else {
					if (((byte)(0xFF&(b<<5)))>=0) {
						//code point 26
						result = (b&0x03);
					} else {
						if (((byte)(0xFF&(b<<6)))>=0) {
							//code point 31
							result = (b&0x01);
						} else {
							//System.err.println("odd byte :"+Integer.toBinaryString(b)+" at pos "+(offset-1));
							//the high bit should never be set
							target[targetIdx] = 0xFFFD; //Bad data replacement char
							position+=5; 
							return; 
						}
						
						if ((source[position]&0xC0)!=0x80) {
							target[targetIdx] = 0xFFFD; //Bad data replacement char
							position+=5; 
							return; 
						}
						result = (result<<6)|(source[position++]&0x3F);
					}						
					if ((source[position]&0xC0)!=0x80) {
						target[targetIdx] = 0xFFFD; //Bad data replacement char
						position+=4; 
						return; 
					}
					result = (result<<6)|(source[position++]&0x3F);
				}
				if ((source[position]&0xC0)!=0x80) {
					target[targetIdx] = 0xFFFD; //Bad data replacement char
					position+=3; 
					return; 
				}
				result = (result<<6)|(source[position++]&0x3F);
			}
			if ((source[position]&0xC0)!=0x80) {
				target[targetIdx] = 0xFFFD; //Bad data replacement char
				position+=2;
				return; 
			}
			result = (result<<6)|(source[position++]&0x3F);
		}
		if ((source[position]&0xC0)!=0x80) {
			target[targetIdx] = 0xFFFD; //Bad data replacement char
			position+=1;
			return; 
		}
		target[targetIdx] = (char)((result<<6)|(source[position++]&0x3F));
	}


	public boolean isEOF() {
		fetch(0);
		return (bytesReadyToParse()>0)? false: input.isEOF();
	}
	
}
