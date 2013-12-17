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
	final byte[] buffer; //TODO: build an Unsafe version of Reader and Writer for fastest performance on server.
	private final byte[] pmapStack;	
	private int pmapStackDepth = 0;
	
	private int position;
	private int limit;
	
	//both bytes but class def likes int much better for alignment
	private int pmapIdx = -1; //-1 -> 7
	private int bitBlock = 0;
	
	
	public void reset() {
		totalReader = 0;
		position = 0;
		limit = 0;
		pmapStackDepth = 0;
		pmapIdx = -1;
		
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
		
		this.pmapStack = new byte[maxPMapCount];//all pmap bytes in total for maximum depth.
		input.init(new DataTransfer(this));
	}
	
	public long totalRead() {
		return totalReader;
	}
	
	private final void fetch(int need) {
		//System.err.println("need more");
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
	
	public final byte[] getBuffer() {
		return buffer;
	}
	
	public final void readByteData(byte[] target, int offset, int length) {
		//ensure all the bytes are in the buffer before calling visitor
		if (position>limit - length) {
			fetch(length);
		}
		System.arraycopy(buffer, position, target, offset, length);
		position+=length;
	}
	
	//By writing pmap into byteconsumer we have a vitual lookup for the next byte
	//if it can be kept here then it can be inlined instead however groups
	//will often nest which will require a stack of pmaps.
	
	//*** this is the best idea! go with it. Flaw found with each of the others.
	
	//its not "REALLY" a stack its just a pre-empt of new bits to be read before
	//the rest of the list is finished so if all new are kept in an array as we
	//work our way down we can just add more on at that point then when we get
	//back to the old location it will naturally flow!
	
	//the zeros and dynamic length may pose a problem.
	//also stopping at non by boundaries for next group will pose a problem.
	//leave stop bits in bytes?
	
	/////
	//leave stop bit so we know when to stop and return zeros
	//group will call popPmap() at end of fields
	//must write pmap bytes onto list backwards.
	//* we know MAX of bits when reading but it may be shorter.
	//* read ahead like strings, buffer is holding max bytes.
	//each pmap needs a bit position byte on another stack. one per pmap only
	//OR each pmap byte is decoded in a method that pushes 7 valeus on or 8 if stop!!
	
	//TODO: duplicate this for the writer logic accumulating bits to be written on a single list
	//TODO: write unit tests around these functions.
	

	
	//called at the start of each group unless group knows it has no pmap
	public final void readPMap(int pmapMaxSize) {
		//force internal buffer to grow if its not big enough for this pmap
		if (limit - position < pmapMaxSize) {
			fetch(pmapMaxSize); //largest fetch
		}
		//there are no zero length pmaps these are determined by the parent pmap
		int start = position;
		byte[] b = buffer;
		int p = position;
		
		//scan for index of the stop bit.
		int validation = pmapMaxSize;
		do {			
			if (--validation<0) {
				throw new FASTException("Can not find end of PMAP in given max byte length of "+pmapMaxSize+".");
			}
		} while (b[p++]>=0);
		
		//push the old index for resume
		if (pmapIdx>0) {
			pmapStack[pmapStackDepth++] = (byte)pmapIdx;
		}
		
		position = p;				
		//walk back wards across these and push them on the stack
		//the first bits to read will the the last thing put on the array
		int j = position;
		
		while (--j>=start) {
			pmapStack[pmapStackDepth++] = b[j];
		}
		bitBlock = pmapStack[pmapStackDepth-1];
		
		//set next bit to read
		pmapIdx = 7;
		
	}

	//called at every field to determine operation
	public final byte popPMapBit() {
		if (pmapIdx<0) {
			//must return all the trailing zeros for the bit map after hit end of map. see (a1)
			return 0;
		}
		//byte block = pmapStack[pmapStackDepth-1];
		//get next bit and decrement the bit index pmapIdx
		byte value = (byte)(1&(bitBlock>>>(--pmapIdx)));
		if (pmapIdx==0) {
			resetToNextSeven();
		}
		return value;
	}

	private void resetToNextSeven() {
		pmapIdx = 7;
		//if we have not reached the end of the map dec to the next byte
		if (bitBlock >= 0) {
			pmapStackDepth--;
			bitBlock = pmapStack[pmapStackDepth-1];
		} else {
			//(a1) hit end of map, set this to < 0 so we return zeros until this pmap is popped off.
			pmapIdx = -1;
		}
	}	
	
	//called at the end of each group
	public final void popPMap() {
		if (pmapStackDepth>2) {
			pmapStackDepth--;
			pmapIdx = pmapStack[--pmapStackDepth];
			bitBlock = pmapStack[pmapStackDepth-1];
		}
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
		if (limit-position<=6) {
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
		
		int p = position;
		byte[] buff = this.buffer;
		
		
		byte v = buff[p++];
		int accumulator = ((v&0x40)==0) ? 0 :0xFFFFFF80;

	    while (v>=0) {  //(v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buff[p++];
	    }
	    position = p;
	    return accumulator|(v&0x7F);
	}
	
	public final int readIntegerUnsignedOptional() {
		return readIntegerUnsigned()-1;
	}
	
	public final int readIntegerUnsigned() {
		if (position>limit-6) {
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

	public boolean isPMapOpen() {
		return pmapStackDepth>0;
	}

	public void readTextASCII(Appendable target) {
		if (limit - position < 2) {
			fetch(2);
		}
		
		//can read maxLength with no worry
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

	public int readTextASCII(char[] target, int offset) {
		if (limit - position < 2) {
			fetch(2);
		}
		
		//can read maxLength with no worry
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
				target[offset++]=(char)(buffer[position++]);
				if (position>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
			}
			target[offset++]=(char)(0x7F & buffer[position++]);
		}
		return offset;//TODO: return new position instead of length? confirm this is right?
	}

	
	//TODO: if this does not perform well after in-line remove the interface and return to concrete
	public void readTextUTF8(int charCount, Appendable target) {
		while (--charCount>=0) {
			byte b = buffer[position++];
			if (b>=0) {
				//code point 7
				try {
					target.append((char)b);
				} catch (IOException e) {
					throw new FASTException(e);
				}
			} else {
				decodeUTF8(buffer, target);
			}
		}
	}
	
	public void readTextUTF8(char[] target, int offset, int charCount) {
		while (--charCount>=0) {
			byte b = buffer[position++];
			if (b>=0) {
				//code point 7
				target[offset++] = (char)b;
			} else {
				decodeUTF8(buffer, target, offset++);
			}
		}
	}
	
	//convert single char that is not the simple case
		private void decodeUTF8(byte[] source, Appendable target) {

			byte b = source[position-1];
		    int result;
			if ( ((byte)(0xFF&(b<<2))) >=0) {
				if ((b&0x40)==0) {
					try {
						target.append((char)0xFFFD); //Bad data replacement char
					} catch (IOException e) {
						throw new FASTException(e);
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
								position+=5; 
								return; 
							}
							
							if ((source[position]&0xC0)!=0x80) {
								try {
									target.append((char)0xFFFD); //Bad data replacement char
								} catch (IOException e) {
									throw new FASTException(e);
								}
								position+=5; 
								return; 
							}
							result = (result<<6)|(source[position++]&0x3F);
						}						
						if ((source[position]&0xC0)!=0x80) {
							try{
								target.append((char)0xFFFD); //Bad data replacement char
							} catch (IOException e) {
								throw new FASTException(e);
							}
							position+=4; 
							return; 
						}
						result = (result<<6)|(source[position++]&0x3F);
					}
					if ((source[position]&0xC0)!=0x80) {
						try {
							target.append((char)0xFFFD); //Bad data replacement char
						} catch (IOException e) {
							throw new FASTException(e);
						}
						position+=3; 
						return; 
					}
					result = (result<<6)|(source[position++]&0x3F);
				}
				if ((source[position]&0xC0)!=0x80) {
					try {
						target.append((char)0xFFFD); //Bad data replacement char
					} catch (IOException e) {
						throw new FASTException(e);
					}
					position+=2;
					return; 
				}
				result = (result<<6)|(source[position++]&0x3F);
			}
			if ((source[position]&0xC0)!=0x80) {
				try {
					target.append((char)0xFFFD); //Bad data replacement char
				} catch (IOException e) {
					throw new FASTException(e);
				}
				position+=1;
				return; 
			}
			try {
				target.append((char)((result<<6)|(source[position++]&0x3F)));
			} catch (IOException e) {
				throw new FASTException(e);
			}
		}
	
	//convert single char that is not the simple case
	private void decodeUTF8(byte[] source, char[] target, int targetIdx) {

		byte b = source[position-1];
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
