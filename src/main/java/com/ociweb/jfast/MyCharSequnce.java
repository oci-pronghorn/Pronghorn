package com.ociweb.jfast;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class MyCharSequnce implements CharSequence, ByteConsumer {

	private char[] seq;
	private int limit;
	//private CharSequence prefix;//NONE
	
	//TODO: perhaps another interface may be good for streaming the write here
	public MyCharSequnce() {
		seq = new char[PrimitiveReader.VERY_LONG_STRING_MASK];//make the same optimization assumption here
		limit = 0;
	}
	
	public final void put(byte value) {
		//grow to the biggest string found in stream then stop growing.
		if (limit >= seq.length) {
			grow(limit);
		}
		seq[limit++]=(char)value;
	}
	

	public final void reset(byte[] source, int offset, int total, int plusOne) {
		
		//reset always sets limit back to zero.
		if(total >= seq.length) {
			grow(total);
		}

		int i = total-1;
		int j = offset+i;
		seq[i] = (char) plusOne;
		while (--i>=0) {//can not use array copy due to cast
			seq[i] = (char) source[--j];
		}
		limit = total;
	}

	private void grow(int target) {
		int newCapacity = target*2;
		char[] newSeq = new char[newCapacity];
		System.arraycopy(seq, 0, newSeq, 0, seq.length);
		seq = newSeq;
	}
	
	//package protect this method for write.
	public final void put(char value) {
		//grow to the biggest string found in stream then stop growing.
		if (limit>=seq.length) {
			grow(limit);
		}
		seq[limit++]=value;
	}

	
	public final int length() {
		return limit;
	}

	public final char charAt(int index) {
		return seq[index];
	}

	public CharSequence subSequence(int start, int end) {
		// TODO Auto-generated method stub
		return null;
	}

	public String toString() {
		return new String(seq, 0, limit);
	}
	
	public final void clear() {
		limit = 0;
	}

	public CharSequence setValue(CharSequence value) {
		if (null==value) {//not sure this belongs here.
			clear();
			return null;
		}
		int i = value.length();
		limit = i;
		if (limit>=seq.length) {
			grow(limit);
		}
		while (--i>=0) {
			seq[i]=value.charAt(i);
		}
		return this;
	}
	
	//convert to full byte array from char array
	public void encodeUTF8(char[] source, int sourceOffset, int charCount, byte[] target, int targetOffset) {
		while (--charCount>=0) {
			int c = source[sourceOffset++];
			
			if (c<=0x007F) {
				//code point 7
				target[targetOffset++] = (byte)c;
			} else {
				if (c<=0x07FF) {
					//code point 11
					target[targetOffset++] = (byte)(0xC0|((c>>6)&0x1F));
				} else {
					if (c<=0xFFFF) {
						//code point 16
						target[targetOffset++] = (byte)(0xE0|((c>>12)&0x0F));
					} else {
						if (c<0x1FFFFF) {
							//code point 21
							target[targetOffset++] = (byte)(0xF0|((c>>18)&0x07));
						} else {
							if (c<0x3FFFFFF) {
								//code point 26
								target[targetOffset++] = (byte)(0xF8|((c>>24)&0x03));
							} else {
								if (c<0x7FFFFFFF) {
									//code point 31
									target[targetOffset++] = (byte)(0xFC|((c>>30)&0x01));
								} else {
									throw new UnsupportedOperationException("can not encode char with value: "+c);
								}
								target[targetOffset++] = (byte)(0x80 |((c>>24) &0x3F));
							}
							target[targetOffset++] = (byte)(0x80 |((c>>18) &0x3F));
						}						
						target[targetOffset++] = (byte)(0x80 |((c>>12) &0x3F));
					}
					target[targetOffset++] = (byte)(0x80 |((c>>6) &0x3F));
				}
				target[targetOffset++] = (byte)(0x80 |((c)   &0x3F));
			}
		}		
	}
	
	
	//convert to full char array from byte array
	public void decodeUTF8(byte[] source, int offset, char[] target, int charTarget, int charCount) {
		while (--charCount>=0) {
			byte b = source[offset++];
			if (b>=0) {
				//code point 7
				target[charTarget++] = (char)b;
			} else {
			    offset = decodeUTF8(source, offset, target, charTarget++);
			}
			//System.err.println(target[charTarget-1]);
		}
	}

	//convert single char that is not the simple case
	private int decodeUTF8(byte[] source, int offset, char[] target, int targetIdx) {

		byte b = source[offset-1];
	    int result;
		if ( ((byte)(0xFF&(b<<2))) >=0) {
			if ((b&0x40)==0) {
				target[targetIdx] = 0xFFFD; //Bad data replacement char
				return ++offset;
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
							return offset+5; 
						}
						
						if ((source[offset]&0xC0)!=0x80) {
							target[targetIdx] = 0xFFFD; //Bad data replacement char
							return offset+5; 
						}
						result = (result<<6)|(source[offset++]&0x3F);
					}						
					if ((source[offset]&0xC0)!=0x80) {
						target[targetIdx] = 0xFFFD; //Bad data replacement char
						return offset+4; 
					}
					result = (result<<6)|(source[offset++]&0x3F);
				}
				if ((source[offset]&0xC0)!=0x80) {
					target[targetIdx] = 0xFFFD; //Bad data replacement char
					return offset+3; 
				}
				result = (result<<6)|(source[offset++]&0x3F);
			}
			if ((source[offset]&0xC0)!=0x80) {
				target[targetIdx] = 0xFFFD; //Bad data replacement char
				return offset+2;
			}
			result = (result<<6)|(source[offset++]&0x3F);
		}
		if ((source[offset]&0xC0)!=0x80) {
			target[targetIdx] = 0xFFFD; //Bad data replacement char
			return offset+1;
		}
		target[targetIdx] = (char)((result<<6)|(source[offset++]&0x3F));
		return offset;
	}


}
