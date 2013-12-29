package com.ociweb.jfast.field;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.jfast.error.FASTException;

/**
 * Manage all the text (char sequences) for all the fields.
 * * Must maintain a minimum count of pointers to reduce GC overhead.
 * * Must be garbage free and not release objects/arrays unless reporting an error.
 * * Must work within the fixed size given or throw.
 * * Must support all the dynamic behavior needed by the operators.
 * * Must support constant time access to each block of text.
 * 
 * @author Nathan Tippy
 *
 */
public class TextHeap {

	private int totalContent = 0; //total chars consumed by current text.
	private int totalWorkspace = 0; //working space around each text.
	
	private final int gapCount;
	private final char[] data;
	private final int dataLength;
	
	private final int[] initTat;
	private final char[] initBuffer;
	
	//remain true unless memory gets low and it has to give up any margin
	private boolean preserveWorkspace = true;
	private final int textItemCount;
	
	//text allocation table
	private final int[] tat;
	//4 ints per text body.
	//start index position (inclusive)
	//stop index limit (exclusive)
	//max prefix append
	//max postfix append
	
	
	TextHeap(int singleTextSize, int singleGapSize, int fixedTextItemCount) {
		this(singleTextSize,singleGapSize,fixedTextItemCount,0,new int[0],new char[0][]);
	}
	
	
	TextHeap(int singleTextSize, int singleGapSize, int fixedTextItemCount, 
			int charInitTotalLength, int[] charInitIndex, char[][] charInitValue) {
		
		textItemCount = fixedTextItemCount;
		
		gapCount = fixedTextItemCount+1;
		dataLength = (singleGapSize*gapCount)+(singleTextSize*fixedTextItemCount);
		data = new char[dataLength];
		tat = new int[fixedTextItemCount<<2];
		initTat = new int[fixedTextItemCount<<1];
		
		
		int i = tat.length;
		int j = dataLength+(singleTextSize>>1);
		while (--i>=0) {
			int idx = (j-=(singleTextSize+singleGapSize));
			i--;
			i--;
			tat[i--]=idx;
			tat[i]=idx;		
		}	
		

		if (null==charInitValue || charInitValue.length==0) {
			initBuffer = null;
		} else {
			initBuffer= new char[charInitTotalLength];
			
			int stopIdx = charInitTotalLength;
			int startIdx = stopIdx;
			
			i = charInitValue.length;
			while (--i>=0) {
				int len = null==charInitValue[i]?0:charInitValue[i].length;
				
				
				startIdx -= len;	
				if (len>0) {
					System.arraycopy(charInitValue[i], 0, initBuffer, startIdx, len);
				}
				//will be zero zero for values without constants.
				int offset = i<<1;
				initTat[offset] = startIdx;
				initTat[offset+1] = stopIdx;
										
				stopIdx = startIdx;
			}	
		}
	}
	

	public void reset() {
	
		int i = textItemCount;
		while (--i>=0) {
			
			int b = i<<1;
						
			if (initTat[b]==initTat[b+1]) {
				setNull(i);				
			} else {
				set(i, initBuffer, initTat[b], initTat[b+1]-initTat[b]);
				
			}
			
		}
	}
	
	
	//Caution: this method will create a new String instance
	public CharSequence getSub(int idx, int start, int end) {
		int offset = idx<<2;
		return new String(data,tat[offset]+start,Math.min(tat[offset+1], tat[offset]+end ));
	}
	
	void setZeroLength(int idx) {
		int offset = idx<<2;
		tat[offset+1] = tat[offset];
	}
	
	void setNull(int idx) {
		int offset = idx<<2;
		tat[offset+1] = tat[offset]-1;
	}
	
	public boolean isNull(int idx) {
		int offset = idx<<2;
		return tat[offset+1] == tat[offset]-1;
	}
	
	//simple replacement of last value
	//since the old value is tossed this opportunity is taken to re-center the text
	//between the one in front and the one behind.
	//if this is large it may need to move surrounding text and 
	//may throw if there is no more room in the heap.
	void set(int idx, char[] source, int sourceIdx, int sourceLen) {
		
		int offset = idx<<2;
		
		int prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
		int nextTextStart = offset+4 >= tat.length  ? dataLength  : tat[offset+4];
		int space = nextTextStart - prevTextStop;
		int middleIdx = (space-sourceLen)>>1;
		
		if (sourceLen<=space) {
			simpleReplace(source, sourceIdx, sourceLen, offset, prevTextStop, middleIdx);
		} else {
			//we do not have enough space move to the bigger side.
			makeRoom(offset, middleIdx>(dataLength>>1), (sourceLen + tat[offset+2] + tat[offset+3])-space);
            
    		prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
    		nextTextStart = offset+4 >= dataLength ? dataLength : tat[offset+4];
    		
            simpleReplace(source, sourceIdx, sourceLen, offset, prevTextStop, ((nextTextStart-prevTextStop)-sourceLen)>>1);         
		}
	}

	void set(int idx, CharSequence charSequence) {
		
		int offset = idx<<2;
		
		int sourceLen = charSequence.length();
		int prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
		int nextTextStart = offset+4 >= tat.length  ? dataLength  : tat[offset+4];
		int space = nextTextStart - prevTextStop;
		int middleIdx = (space-sourceLen)>>1;
		
		if (sourceLen<=space) {
			simpleReplace(charSequence, offset, prevTextStop, middleIdx);
		} else {
			//we do not have enough space move to the bigger side.
			makeRoom(offset, middleIdx>(dataLength>>1), (sourceLen + tat[offset+2] + tat[offset+3])-space);
            
    		prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
    		nextTextStart = offset+4 >= dataLength ? dataLength : tat[offset+4];
    		
            simpleReplace(charSequence, offset, prevTextStop, ((nextTextStart-prevTextStop)-sourceLen)>>1);         
		}
	}
	
	private void makeRoom(int offsetNeedingRoom, boolean startBefore, int totalDesired) {

		if (startBefore) {
			totalDesired = makeRoomBeforeFirst(offsetNeedingRoom, totalDesired);			
		} else {
			totalDesired = makeRoomAfterFirst(offsetNeedingRoom, totalDesired);
		}
		if (totalDesired>0) {
			throw new RuntimeException("TextHeap must be initialized with more ram for required text");
		}
	}

	private int makeRoomAfterFirst(int offsetNeedingRoom, int totalDesired) {
		//compress rear its larger
		totalDesired = compressAfter(offsetNeedingRoom+4, totalDesired, preserveWorkspace);
		if (totalDesired>0) {
			totalDesired = compressBefore(offsetNeedingRoom-4, totalDesired, preserveWorkspace);
			if (totalDesired>0) {
				preserveWorkspace = false;
				totalDesired = compressAfter(offsetNeedingRoom+4, totalDesired, preserveWorkspace);
				if (totalDesired>0) {
					totalDesired = compressBefore(offsetNeedingRoom-4, totalDesired, preserveWorkspace);
				}
			}
		}
		return totalDesired;
	}

	private int makeRoomBeforeFirst(int offsetNeedingRoom, int totalDesired) {
		//compress front its larger
		totalDesired = compressBefore(offsetNeedingRoom-4, totalDesired, preserveWorkspace);
		if (totalDesired>0) {
			totalDesired = compressAfter(offsetNeedingRoom+4, totalDesired, preserveWorkspace);
			if (totalDesired>0) {
				preserveWorkspace = false;
				totalDesired = compressBefore(offsetNeedingRoom-4, totalDesired, preserveWorkspace);
				if (totalDesired>0) {
					totalDesired = compressAfter(offsetNeedingRoom+4, totalDesired, preserveWorkspace);
				}
			}
		}
		return totalDesired;
	}

	private void simpleReplace(char[] source, int sourceIdx, int sourceLen, int offset, int prevTextStop, int middleIdx) {
		//write and return because we have enough space
		int target = prevTextStop+middleIdx;
		if (target<0) {
			target=0;
		}
		int limit = target+sourceLen;
		if (limit>dataLength) {
			target = dataLength-sourceLen;
		}
		
		System.arraycopy(source, sourceIdx, data, target, sourceLen);
		//full replace
		totalContent+=(sourceLen-(tat[offset+1]-tat[offset]));
		tat[offset] = target;
		tat[offset+1] = limit;
	}
	
	private void simpleReplace(CharSequence charSequence, int offset, int prevTextStop, int middleIdx) {
		//write and return because we have enough space
		int target = prevTextStop+middleIdx;
		if (target<0) {
			target=0;
		}
		int limit = target+charSequence.length();
		if (limit>dataLength) {
			target = dataLength-charSequence.length();
		}
		
		int j = target+charSequence.length();
		int i = charSequence.length();
		while (--i>=0) {
			data[--j] = charSequence.charAt(i);
		}
		//full replace
		totalContent+=(charSequence.length()-(tat[offset+1]-tat[offset]));
		tat[offset] = target;
		tat[offset+1] = limit;
	}
	
	private int compressBefore(int stopOffset, int totalDesired, boolean preserveWorkspace) {
		//moving everything to the left until totalDesired
		//start at zero and move everything needed if possible.
		int dataIdx = 0;
		int offset = 0;
		while (offset<=stopOffset) {
			
			int leftBound = 0;
			int rightBound = 0;
			int textLength = tat[offset+1] - tat[offset]; 
			int totalNeed = textLength;
			
			if (preserveWorkspace) {
				int padding = 0;
				leftBound = tat[offset+2]; 
				rightBound = tat[offset+3];
				
				padding += leftBound;
				padding += tat[offset+3];
				int avgPadding = (dataLength - (this.totalContent+this.totalWorkspace))/gapCount;
				if (avgPadding<0) {
					avgPadding = 0;
				}
				
				padding += avgPadding;
				
				int halfPad = avgPadding>>1;
				leftBound += halfPad;
				rightBound += halfPad;
				
				totalNeed += padding;
			}
					
			int copyTo = dataIdx+leftBound;
			
			if (copyTo<tat[offset]) {
				//copy down and save some room.
				System.arraycopy(data, tat[offset], data, copyTo, textLength);
				tat[offset] = copyTo;
				int oldStop = tat[offset+1];
				int newStop = copyTo+textLength;
				tat[offset+1] = newStop;
				
				totalDesired -= (oldStop-newStop);
				
				dataIdx = dataIdx+totalNeed;
			} else {
				//it is already lower than this point.
				dataIdx = tat[offset+1]+rightBound;
			}
			offset+=4;
		}
	
		return totalDesired;
	}

	private int compressAfter(int stopOffset, int totalDesired, boolean preserveWorkspace) {
		//moving everything to the right until totalDesired
		//start at zero and move everything needed if possible.
		int dataIdx = dataLength;
		int offset = tat.length-4;
		while (offset>=stopOffset) {
			
			int leftBound = 0;
			int rightBound = 0;
			int textLength = tat[offset+1] - tat[offset]; 
			int totalNeed = textLength;
			
			if (preserveWorkspace) {
				int padding = 0;
				leftBound = tat[offset+2]; 
				rightBound = tat[offset+3];
				
				padding += leftBound;
				padding += tat[offset+3];
				int avgPadding = (dataLength - (this.totalContent+this.totalWorkspace))/gapCount;
				if (avgPadding<0) {
					avgPadding = 0;
				}
				
				padding += avgPadding;
				
				int halfPad = avgPadding>>1;
				leftBound += halfPad;
				rightBound += halfPad;
				
				totalNeed += padding;
			}
					
			int newStart = dataIdx-(rightBound+textLength);
			StringBuilder builder = new StringBuilder();
			inspectHeap(builder);
			//System.err.println("before:"+builder.toString());
			if (newStart>tat[offset]) {
				//copy up and save some room.
				System.arraycopy(data, tat[offset], data, newStart, textLength);
				int oldStart = tat[offset];
				tat[offset] = newStart;
				totalDesired -= (newStart-oldStart);
				tat[offset+1] = newStart+textLength;
				dataIdx = dataIdx - totalNeed;
			} else {
				//it is already greater than this point.
				dataIdx = tat[offset]-leftBound;
			}
			
			builder.setLength(0);
			inspectHeap(builder);
			//System.err.println("after :"+builder.toString());
			
			offset-=4;
		}
	
		return totalDesired;
	}

	private void inspectHeap(StringBuilder target) {
		target.append('[');
		int i=0;
		while (i<dataLength) {
			if (data[i]==0) {
				target.append('_');
			} else {
				target.append(data[i]);
			}
			i++;			
		}
		target.append(']');
		
	}

	void appendTail(int idx, char value) {
		//if not room make room checking after first because thats where we want to copy the tail.
		int offset = idx<<2;
		
		if (tat[offset]>tat[offset+1]) {
			//null or empty string detected so change to simple set
			tat[offset+1]=tat[offset];
		}
				
		int stop = tat[offset+1]+1;
		int limit = offset+4<tat.length ? tat[offset+4] : dataLength;
		
		if (stop>=limit) {
			int floor = offset-3>=0 ? tat[offset-3] : 0;
			int space = limit- floor;
			int need = stop - tat[offset];
			
			if (need>space) {
				makeRoom(offset, false, need);			
			}
			//we have some space so just shift the existing data.
			int len = tat[offset+1]-tat[offset];
			System.arraycopy(data, tat[offset], data, floor, len);
			tat[offset] = floor;
			tat[offset+1] = floor+len;
		}
	
		//everything is now ready to trim and copy.
		data[tat[offset+1]++] = value;
	}
	
	
	//append chars on to the end of the text after applying trim
	//may need to move existing text or following texts
	//if there is no room after moving everything throws
	void appendTail(int idx, int trimTail, char[] source, int sourceIdx, int sourceLen) {
		//if not room make room checking after first because thats where we want to copy the tail.
		int offset = idx<<2;
		
		if (tat[offset]>=tat[offset+1]) {
			//null or empty string detected so change to simple set
			set(idx,source,sourceIdx,sourceLen);
			return;
		}
		
		int stop = tat[offset+1]+(sourceLen-trimTail);
		int limit = offset+4<tat.length ? tat[offset+4] : dataLength;
		
		if (stop>=limit) {
			int floor = offset-3>=0 ? tat[offset-3] : 0;
			int space = limit- floor;
			int need = stop - tat[offset];
			
			if (need>space) {
				makeRoom(offset, false, need);			
			}
			//we have some space so just shift the existing data.
			int len = tat[offset+1]-tat[offset];
			System.arraycopy(data, tat[offset], data, floor, len);
			tat[offset] = floor;
			tat[offset+1] = floor+len;

		}
		//everything is now ready to trim and copy.
		
		//keep the max head append size
		int maxTail = tat[offset+3];
		int dif=(sourceLen-trimTail);
		if (dif>maxTail) {
			tat[offset+3] = dif;
		}
			
		//everything is now ready to trim and copy.
		int targetPos = tat[offset+1]-trimTail;
		System.arraycopy(source, sourceIdx, data, targetPos, sourceLen);
		tat[offset+1] = targetPos+sourceLen;
		
	}
	
	//append chars on to the front of the text after applying trim
	//may need to move existing text or previous texts
	//if there is no room after moving everything throws
	void appendHead(int idx, int trimHead, char[] source, int sourceIdx, int sourceLen) {
		//if not room make room checking before first because thats where we want to copy the head.
		int offset = idx<<2;
		
		if (tat[offset]>=tat[offset+1]) {
			//null or empty string detected so change to simple set
			set(idx,source,sourceIdx,sourceLen);
			return;
		}
		
		int start = tat[offset]-(sourceLen-trimHead);
		int limit = offset-3<0 ? 0 : tat[offset-3];
		
		
		if (start<limit) {
			int stop = offset+4<tat.length ? tat[offset+4] : dataLength;
			int space = stop - limit;
			int need = tat[offset+1]-start;
			
			if (need>space) {
				makeRoom(offset, true, need);			
			}
			//we have some space so just shift the existing data.
			int len = tat[offset+1] - tat[offset];
			System.arraycopy(data, tat[offset], data, start, len);
			tat[offset] = start;
			tat[offset+1] = start+len;
			
		}
		//everything is now ready to trim and copy.
		
		//keep the max head append size
		int maxHead = tat[offset+2];
		int dif=(sourceLen-trimHead);
		if (dif>maxHead) {
			tat[offset+2] = dif;
		}
			
		//everything is now ready to trim and copy.
		int newStart = tat[offset]-dif;
		System.arraycopy(source, sourceIdx, data, newStart, sourceLen);
		tat[offset]=newStart;
						
	}
	
	
	///////////
	//////////
	//read access methods
	//////////
	//////////
	

	public char getChar(int idx, int index) {
		return data[tat[idx<<2]];
	}
	
	public int get(int idx, char[] target, int targetIdx) {
		if (idx<0) {
			int offset = idx << 1; //this shift left also removes the top bit! sweet.
			
			int pos = initTat[offset];
			int len = initTat[offset+1]-pos;
			System.arraycopy(initBuffer, pos, target, targetIdx, len);
			return len;
			
		} else {
		
			int offset = idx<<2;
			
			int pos = tat[offset];
			int len = tat[offset+1]-pos;
			
			System.arraycopy(data, pos, target, targetIdx, len);
			return len;
		}
	}
	
	public void get(int idx, Appendable target) {
		if (idx<0) {
			int offset = idx << 1; //this shift left also removes the top bit! sweet.
			
			int pos = initTat[offset];
			int lim = initTat[offset+1];
			
			try {
				while (pos<lim) {
						target.append(initBuffer[pos++]);
				}
			} catch (IOException e) {
				throw new FASTException(e);
			}
			
		} else {
			int offset = idx<<2;
			
			int pos = tat[offset];
			int lim = tat[offset+1];
			
			try {
				while (pos<lim) {
						target.append(data[pos++]);
				}
			} catch (IOException e) {
				throw new FASTException(e);
			}
		}
	}
	
	public int countHeadMatch(int idx, CharSequence value) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		
		int i = 0;
		int limit = Math.min(value.length(),lim-pos);
		while (i<limit && data[pos+i]==value.charAt(i)) {
			i++;
		}
		return i;
	}
	
	public int countTailMatch(int idx, CharSequence value) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		int vlim = value.length();
		
		int limit = Math.min(vlim,lim-pos);
		int i = 1;
		while (i<=limit && data[lim-i]==value.charAt(vlim-i)) {
			i++;
		}
		return i-1;
	}
	

	public boolean equals(int idx, CharSequence value) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		
		int i = value.length();
		if (lim-pos!=i) {
			return false;
		}
		while (--i>=0) {
			if (value.charAt(i)!=data[pos+i]) {
				return false;
			}
		}
		return true;
	}
	
	public int countHeadMatch(int idx, char[] source, int sourceIdx, int sourceLength) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		
		int i = 0;
		int limit = Math.min(sourceLength,lim-pos);
		while (i<limit && data[pos+i]==source[sourceIdx+i]) {
			i++;
		}
		return i;
	}
	
	public int countTailMatch(int idx, char[] source, int sourceIdx, int sourceLength) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		int vlim = sourceLength;
		
		int limit = Math.min(vlim,lim-pos);
		int i = 1;
		int last = sourceLength+sourceIdx;
		while (i<=limit && data[lim-i]==source[last-i]) {
			i++;
		}
		return i-1;
	}
	
	public boolean equals(int idx, char[] source, int sourceIdx, int sourceLength) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		
		int i = sourceLength;
		if (lim-pos!=i) {
			return false;
		}
		int j = pos+i;
		while (--i>=0) {
			if (source[sourceIdx+i]!=data[--j]) {
				return false;
			}
		}
		return true;
	}

	public int textCount() {
		return tat.length>>2;
	}
	
	public int length(int idx) {
		int offset = idx<<2;
		return tat[offset+1] - tat[offset];
	}

	//convert single char that is not the simple case
	private static int decodeUTF8(byte[] source, int offset, char[] target, int targetIdx) {
	
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

	//convert to full char array from byte array
	static void decodeUTF8(byte[] source, int offset, char[] target, int charTarget, int charCount) {
		while (--charCount>=0) {
			byte b = source[offset++];
			if (b>=0) {
				//code point 7
				target[charTarget++] = (char)b;
			} else {
			    offset = TextHeap.decodeUTF8(source, offset, target, charTarget++);
			}
			//System.err.println(target[charTarget-1]);
		}
	}

	//convert to full byte array from char array
	static void encodeUTF8(char[] source, int sourceOffset, int charCount, byte[] target, int targetOffset) {
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





}
