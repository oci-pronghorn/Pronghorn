//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import java.io.IOException;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.rabin.WindowedFingerprint;

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
	
	private final FASTInput input;
	private long totalReader;
	final byte[] buffer;
	final long[] hashBuffer;
	
	private final byte[] invPmapStack;
	private int invPmapStackDepth;
	
	private final boolean useFingerprint = false;
	
	//Hack test to get a feeling for the cost of adding this feature.
	//TODO: this call creates garbage and must not be here in the future.
	WindowedFingerprint windowedFingerprint = com.ociweb.rabin.WindowedFingerprintFactory.buildNew();
	
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


	public PrimitiveReader(FASTInput input) {
		this(4096,input,1024);
	}
	
	public PrimitiveReader(int initBufferSize, FASTInput input, int maxPMapCount) {
		this.input = input;
		this.buffer = new byte[initBufferSize];
		this.hashBuffer = new long[initBufferSize];
		
		this.position = 0;
		this.limit = 0;
		
		this.invPmapStack = new byte[maxPMapCount]; //TODO: need two bytes of gap between each!!! how to external?
		this.invPmapStackDepth = invPmapStack.length-2;
				
		input.init(this.buffer);
	}
	
	public long totalRead() {
		return totalReader;
	}
	public int remaining() {
		return limit-position;
	}
	
	public final void fetch() {
		fetch(1);
	}
	
	//Will not return until the need is met because the parser has
	//determined that we can not continue until this data is provided.
	//this call may however read in more than the need because its ready
	//and convenient to reduce future calls.
	private void fetch(int need) {
		int count = 0;
		need = fetchAvail(need);
		while (need>0) {
			if (0==count++) {
				
				//compact and prep for data spike
				
			} else {
				if (count<10) { 
					Thread.yield();
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
			buildFingerprint(filled);	
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
		
		buildFingerprint(filled);	
		return need-filled;
		
	}
	
	private void buildFingerprint(int c) {
		//This feature will NOT be used when de-multiplexing redundant
		//feeds for hot fail over however it will be needed for 
		//trusted delivery of data to mobile devices that only have 1 
		//incoming feed.
		
		//this works well for the last mile problem into homes or to mobile devices.
		//Home Routers 1mbps-30mbps are common on DSL.
		//3G       600kbps -  3.1 mbps
		//4g         3mbps - 10mbps
		//4glte      5mbps - 12mbps
		//802.11n  100mbps - 300mbps
		
		//without finger prints the stream can easily saturate 500mbps
		//finger prints will add no more than 10% overhead and therefore is
		//a good fit for all these slow networks where it would not be
		//appropriate to send 3 separate streams.
		
		
		if (useFingerprint) {
			//TODO: replace this with garbage free Rabin fingerprints 
			// called on RecordEnd or SequenceBottom
			
			int x = limit-c;
			while (--c>=0) {
				
				//write finger print based on all previous bytes not including this one.
				hashBuffer[x] = windowedFingerprint.fingerprint;
				//now eat this byte
				windowedFingerprint.eat(buffer[x++]);		
			}
		}
	}

	/**
	 * Call this at the 
	 * @return
	 */
	
	public long getFingerprint() {
		return hashBuffer[position];
	}
	
	public final void readByteData(byte[] target, int offset, int length) {
		//ensure all the bytes are in the buffer before calling visitor
		if (position>limit - length) {
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
		
		//force internal buffer to grow if its not big enough for this pmap
//		if (limit - position < pmapMaxSize) {
//			fetch(pmapMaxSize); //largest fetch
//		}

		int k = invPmapStackDepth -= (pmapMaxSize+2);         
    	if (position>=limit) {
			fetch(1);
		}		
		bitBlock = buffer[position];
		if (limit-position>pmapMaxSize) {
	        do {
			} while ((invPmapStack[k++] = buffer[position++])>=0);	
		} else {
			//must use slow path becausee we are near the end of the buffer.
	        do {				
	        	if (position>=limit) {
					fetch(1);
				}				
			} while ((invPmapStack[k++] = buffer[position++])>=0);
		}
        invPmapStack[k] = (byte)(3+pmapMaxSize+(invPmapStackDepth-k));
		
        //set next bit to read
        pmapIdx = 6;
	}

	//called at every field to determine operation
	public final byte popPMapBit() {
		if (pmapIdx>0 || (pmapIdx==0 && bitBlock<0)) {
			//Frequent, 6 out of every 7 plus the last bit block
			return (byte)(1&(bitBlock>>>pmapIdx--));
		} else {
			if (pmapIdx>=0) {
				//SOMETIMES one of 7 we need to move up to the next byte
				pmapIdx = 6;
				byte result = (byte)(1&bitBlock);
				bitBlock = invPmapStack[++invPmapStackDepth];
				return result;
			} else {
				return 0;
			}
		}
	}

	//called at the end of each group
	public final void closePMap() {
		//assert(bitBlock<0);
		//assert(invPmapStack[invPmapStackDepth+1]>=0);
		
		bitBlock = invPmapStack[invPmapStackDepth += (invPmapStack[invPmapStackDepth+1])];
		pmapIdx = invPmapStack[invPmapStackDepth-1];
		
		//
		//TODO: need to add hash at this point for close of group.
		//Or if we use the external length id this can be done by FASTInput.
		
	}
	
	/////////////////////////////////////
	/////////////////////////////////////
	/////////////////////////////////////
	
	public final long readLongSigned () {
		if (limit-position<=10) {
			return readLongSignedSlow();
		}
		
		int p = position;
				
		long v = buffer[p++];
						
		long accumulator = ((v&0x40)==0) ? 0l :0xFFFFFFFFFFFFFF80l;
 
		while (v>=0) { 
			accumulator = (accumulator|v)<<7;
			v = buffer[p++];
		}
    
	    position = p;
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
		if (position>limit-5) {//near the end so must do it the slow way?
			return readIntegerUnsignedSlow();
		}
		byte v = buffer[position++];
		int accumulator;
		if (v>=0) {//(v & 0x80)==0) {
			accumulator = v<<7;
		} else {
			return (v&0x7F);
		}
		
		v = buffer[position++];
	    while (v>=0) { //(v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buffer[position++];
	    }
	    return accumulator|(v&0x7F);
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
	
}
