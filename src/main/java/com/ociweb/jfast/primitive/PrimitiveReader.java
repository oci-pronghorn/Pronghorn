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

	//TODO: must add skip bytes methods
	
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
	
	public PrimitiveReader(FASTInput input) {
		this(4096,input,1024);
	}
	
	//TODO: extract buffer wrapper so unsafe can be injected here.
	public PrimitiveReader(int initBufferSize, FASTInput input, int maxPMapCount) {
		this.input = input;
		this.buffer = new byte[initBufferSize];
		this.position = 0;
		this.limit = 0;
		
		this.invPmapStack = new byte[maxPMapCount]; //TODO: need two bytes of gap between each!!! how to external?
		this.invPmapStackDepth = invPmapStack.length-2;
		
		input.init(new DataTransfer(this));
	}
	
	public long totalRead() {
		return totalReader;
	}
	
	private final void fetch(int need) {
		if (position >= limit) {
			position = limit = 0;
		}
		int remainingSpace = buffer.length-limit;
		if (need <= remainingSpace) {	
			//fill remaining space if possible to reduce fetch later
			int filled = input.fill(buffer, limit, remainingSpace);
			totalReader += filled;
			limit += filled;
		} else {
			noRoomOnFetch(need);
		}
	
	}

	private void noRoomOnFetch(int need) {
		//not enough room at end of buffer for the need
		int populated = limit - position;
		int reqiredSize = need + populated;
		if (buffer.length<reqiredSize) {
			//max value must be computed before startup.
			throw new UnsupportedOperationException("internal buffer is not large enough, requres "+reqiredSize+" bytes");
		} else {
			
			System.arraycopy(buffer, position, buffer, 0, populated);
		}
		//fill and return
		int filled = input.fill(buffer, populated, buffer.length - populated);
		totalReader+=filled;
		position = 0;
		limit = populated+filled;
	}

	
	public final int readBytesPosition(int length) {
		//ensure all the bytes are in the buffer before calling visitor
		if (position>limit - length) {
			fetch(length);
		}
		int result = position;
		position+=length;
		return result;
	}
	
	public final void readByteData(byte[] target, int offset, int length) {
		if (length<0) {
			throw new ArrayIndexOutOfBoundsException("length must be positive but found "+length);
		}
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
	public final void readPMap(final int pmapMaxSize) {
		//push the old index for resume
		invPmapStack[invPmapStackDepth-1] = (byte)pmapIdx;
		
		//force internal buffer to grow if its not big enough for this pmap
		if (limit - position < pmapMaxSize) {
			fetch(pmapMaxSize); //largest fetch
		}

		int k = invPmapStackDepth -= (pmapMaxSize+2);         
				
		bitBlock = buffer[position];
        do {
		} while ((invPmapStack[k++] = buffer[position++])>=0);
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
	public final void popPMap() {
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
	
	
	public final long readLongSignedOptional() {
		//TODO:rewrite
		long temp = readLongSigned();
		if (temp>0) {
			return temp-1;
		}
		return temp;
	}
	
	public final long readLongSigned () {
		if (limit-position<=10) {
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
		
		int p = position;
		byte[] buff = this.buffer;
		
		
		byte v = buff[p++];
		long accumulator = ((v&0x40)==0) ? 0 :0xFFFFFFFFFFFFFF80l;

	    while (v>=0) { //(v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buff[p++];
	    }
	    position = p;
	    return accumulator|(v&0x7F);
	}
	
	public final long readLongUnsignedOptional() {
		return readLongUnsigned()-1;
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
	
	public final int readIntegerSignedOptional() {
		int temp = readIntegerSigned();
		return (temp>0 ? temp-1 : temp);
	}
	
	public final int readIntegerSigned () {
		if (limit-position<=5) {
			return readIntegerSignedSlow();
		}
		
		byte v = buffer[position++];
		int accumulator = ((v&0x40)==0) ? 0 :0xFFFFFF80;

	    while (v>=0) {  //(v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buffer[position++];
	    }
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
	
	public final int readIntegerUnsignedOptional() {
		return readIntegerUnsigned()-1;
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

	public void readTextASCII(Appendable target) {
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
	
	public void readTextUTF8(int charCount, Appendable target) {
		//System.err.println("A");
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
	}
	
	public void readTextUTF8(char[] target, int offset, int charCount) {
		//System.err.println("B");
		byte b;
		if (limit-position>=charCount<<3) { //if bigger than the text could be then use this shortcut
			//fast
			while (--charCount>=0) {
				if ((b = buffer[position++])>=0) {
					//code point 7
					target[offset++] = (char)b;
				} else {
					decodeUTF8Fast(target, offset++, b);
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
