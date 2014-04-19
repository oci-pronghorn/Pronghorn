//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import java.nio.ByteBuffer;


/**
 * Manage all the text (byte sequences) for all the fields.
 * * Must maintain a minimum count of pointers to reduce GC overhead.
 * * Must be garbage free and not release objects/arrays unless reporting an error.
 * * Must work within the fixed size given or throw.
 * * Must support all the dynamic behavior needed by the operators.
 * * Must support constant time access to each block of text.
 * 
 * @author Nathan Tippy
 *
 */
public class ByteHeap {

	private int totalContent = 0; //total bytes consumed by current text.
	private int totalWorkspace = 0; //working space around each text.
	
	private final byte[] data;
	private final int gapCount;
	private final int dataLength;
	private final int tatLength;
	
	private final int[] initTat;
	private final byte[] initBuffer;
	
	//remain true unless memory gets low and it has to give up any margin
	private boolean preserveWorkspace = true;
	private final int itemCount;
	
	//text allocation table
	private final int[] tat;
	//4 ints per text body.
	//start index position (inclusive)
	//stop index limit (exclusive)
	//max prefix append
	//max postfix append
	
	
	public ByteHeap(int singleTextSize, int singleGapSize, int fixedTextItemCount) {
		this(singleTextSize,singleGapSize,fixedTextItemCount,0,new int[0],new byte[0][]);
	}
	
	
	public ByteHeap(int singleTextSize, int singleGapSize, int fixedTextItemCount, 
			int byteInitTotalLength, int[] byteInitIndex, byte[][] byteInitValue) {
		
		itemCount = fixedTextItemCount;
		
		gapCount = fixedTextItemCount+1;
		dataLength = (singleGapSize*gapCount)+(singleTextSize*fixedTextItemCount);
		data = new byte[dataLength];
		tatLength = (fixedTextItemCount<<2);
		tat = new int[tatLength+1];//plus 1 to get dataLength without conditional 
		tat[tatLength]=dataLength;
		initTat = new int[fixedTextItemCount<<1];
				
		int i = tatLength;
		int j = dataLength+(singleTextSize>>1);
		while (--i>=0) {
			int idx = (j-=(singleTextSize+singleGapSize));
			i--;
			i--;
			tat[i--]=idx;
			tat[i]=idx;		
		}	
		

		if (null==byteInitValue || byteInitValue.length==0) {
			initBuffer = null;
		} else {
			initBuffer= new byte[byteInitTotalLength];
			
			int stopIdx = byteInitTotalLength;
			int startIdx = stopIdx;
			
			i = byteInitValue.length;
			while (--i>=0) {
				int len = null==byteInitValue[i]?0:byteInitValue[i].length;			
				startIdx -= len;	
				if (len>0) {
					System.arraycopy(byteInitValue[i], 0, initBuffer, startIdx, len);
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
	
		int i = itemCount;
		while (--i>=0) {
			int b = i<<1;
						
			if (initTat[b]==initTat[b+1]) {
				setNull(i);				
			} else {
				set(i, initBuffer, initTat[b], initTat[b+1]-initTat[b]);
			}			
		}
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

	byte[] rawAccess() {
		return data;
	}
	
	int allocate(int idx, int sourceLen) {
		
		int offset = idx<<2;
		
		int target = makeRoom(offset, sourceLen);

		int limit = target+sourceLen;
		if (limit>dataLength) {
			target = dataLength-sourceLen;
		}
		
		//full replace
		totalContent+=(sourceLen-(tat[offset+1]-tat[offset]));
		tat[offset] = target;
		tat[offset+1] = limit;
		
		return target;
	}


	private int makeRoom(int offset, int sourceLen) {
		int prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
		int nextTextStart = tat[offset+4];
		int space = nextTextStart - prevTextStop;
		int middleIdx = (space-sourceLen)>>1;
		
		if (sourceLen>space) {
			//we do not have enough space move to the bigger side.
			makeRoom(offset, middleIdx>(dataLength>>1), (sourceLen + tat[offset+2] + tat[offset+3])-space);
    		prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
    		nextTextStart = offset+4 >= dataLength ? dataLength : tat[offset+4]; 		
    		middleIdx = ((nextTextStart-prevTextStop)-sourceLen)>>1;         
		}
		
		int target = prevTextStop+middleIdx;
		return target<0 ? 0 : target;
	}
		

	void set(int idx, byte[] source, int startFrom, int copyLength) {
		int offset = idx<<2;
		
		totalContent+=(copyLength-(tat[offset+1]-tat[offset]));
		int target = makeRoom(offset, copyLength);
		tat[offset] = target;
		tat[offset+1] = target+copyLength;
		
		System.arraycopy(source, startFrom, data, target, copyLength);
	}
	
	void set(int idx, ByteBuffer source) {
		int offset = idx<<2;
		
		int copyLength = source.remaining();
		totalContent+=(copyLength-(tat[offset+1]-tat[offset]));
		int target = makeRoom(offset, copyLength);
		tat[offset] = target;
		tat[offset+1] = target+copyLength;
		
		source.mark();
		source.get(data, target, copyLength);
		source.reset();
	}
	
	
	private void makeRoom(int offsetNeedingRoom, boolean startBefore, int totalDesired) {

		if (startBefore) {
			totalDesired = makeRoomBeforeFirst(offsetNeedingRoom, totalDesired);			
		} else {
			totalDesired = makeRoomAfterFirst(offsetNeedingRoom, totalDesired);
		}
		if (totalDesired>0) {
			throw new RuntimeException("Must be initialized with more memory for required text. TotalWorkspace:"+totalWorkspace+" TotalContent:"+totalContent+" DataLength:"+dataLength+" Count:"+itemCount+" need "+totalDesired);
		}
	}

	private int makeRoomAfterFirst(int offsetNeedingRoom, int totalDesired) {
		//compress rear its larger
		totalDesired = compressAfter(offsetNeedingRoom+4, totalDesired);
		if (totalDesired>0) {
			totalDesired = compressBefore(offsetNeedingRoom-4, totalDesired);
			if (totalDesired>0) {
				preserveWorkspace = false;
				totalDesired = compressAfter(offsetNeedingRoom+4, totalDesired);
				if (totalDesired>0) {
					totalDesired = compressBefore(offsetNeedingRoom-4, totalDesired);
				}
			}
		}
		return totalDesired;
	}

	private int makeRoomBeforeFirst(int offsetNeedingRoom, int totalDesired) {
		//compress front its larger
		totalDesired = compressBefore(offsetNeedingRoom-4, totalDesired);
		if (totalDesired>0) {
			totalDesired = compressAfter(offsetNeedingRoom+4, totalDesired);
			if (totalDesired>0) {
				preserveWorkspace = false;
				totalDesired = compressBefore(offsetNeedingRoom-4, totalDesired);
				if (totalDesired>0) {
					totalDesired = compressAfter(offsetNeedingRoom+4, totalDesired);
				}
			}
		}
		return totalDesired;
	}

	
	private int compressBefore(int stopOffset, int totalDesired) {
		//moving everything to the left until totalDesired
		//start at zero and move everything needed if possible.
		int dataIdx = 0;
		int offset = 0;
		while (offset<=stopOffset) {
			
			int leftBound = 0;
			int rightBound = 0;
			int textLength = tat[offset+1] - tat[offset]; 
			if (textLength<0) {
				textLength = 0;
			}
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

	private int compressAfter(int stopOffset, int totalDesired) {
		//moving everything to the right until totalDesired
		//start at zero and move everything needed if possible.
		int dataIdx = dataLength;
		int offset = tatLength-4;
		while (offset>=stopOffset) {
			
			int leftBound = 0;
			int rightBound = 0;
			int textLength = tat[offset+1] - tat[offset]; 
			if (textLength<0) {
				textLength = 0;
			}
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

	//append bytes on to the end of the text after applying trim
	//may need to move existing text or following texts
	//if there is no room after moving everything throws
	void appendTail(int idx, int trimTail, byte[] source, int sourceIdx, int sourceLen) {
		//if not room make room checking after first because thats where we want to copy the tail.
		System.arraycopy(source, sourceIdx, data, makeSpaceForAppend(idx, trimTail, sourceLen), sourceLen);
	}
	
	void appendTail(int idx, int trimTail, ByteBuffer source, int sourceIdx, int sourceLen) {
		//if not room make room checking after first because thats where we want to copy the tail.
		int targetIdx = makeSpaceForAppend(idx, trimTail, sourceLen);
		
		int stop = sourceIdx+sourceLen;
		while (sourceIdx<stop) {
			data[targetIdx++] = source.get(sourceIdx++);
		}
	}
	
	//never call without calling setZeroLength first then a sequence of these
	//never call without calling offset() for first argument
	int appendTail(int offset, int nextLimit, byte value) {
		
		//setZeroLength was called first so no need to check for null 
		if (tat[offset+1] >= nextLimit) {
	//		System.err.println("make space for "+offset);
			makeSpaceForAppend(offset, 1);
			nextLimit = tat[offset+4];
		}
		
		//everything is now ready to trim and copy.
		data[tat[offset+1]++] = value;
		return nextLimit;
	}
	
	void trimTail(int idx, int trim) {
		tat[(idx<<2)+1] -= trim;
	}
	
	void trimHead(int idx, int trim) {
		int offset = idx<<2;
		
		int tmp = tat[offset] + trim;
		int stp = tat[offset+1];
		if (tmp > stp) {
			tmp = stp;
		}
		tat[offset] = tmp;
		
	}
	
	int makeSpaceForAppend(int idx, int trimTail, int sourceLen) {
		int textLen = (sourceLen-trimTail);
		
		int offset = idx<<2;
		
		prepForAppend(offset, textLen);
		//everything is now ready to trim and copy.
		
		//keep the max head append size
		int maxTail = tat[offset+3];
		int dif=(sourceLen-trimTail);
		if (dif>maxTail) {
			tat[offset+3] = dif;
		}
		//target position
		int targetPos = tat[offset+1]-trimTail;
		tat[offset+1] = targetPos+sourceLen;
		return targetPos;
	}

	private void prepForAppend(int offset, int textLen) {
		if (tat[offset]>tat[offset+1]) {
			//switch from null to zero length
			tat[offset+1]=tat[offset];
		}
		
		if (tat[offset+1]+textLen>=tat[offset+4]) {
			makeSpaceForAppend(offset, textLen);
		
		}
	}

	void makeSpaceForAppend(int offset, int textLen) {
		int floor = offset-3>=0 ? tat[offset-3] : 0;
		int need = tat[offset+1]+textLen - tat[offset];
		
		if (need>(tat[offset+4] - floor)) {
			makeRoom(offset, false, need);			
		}
		//we have some space so just shift the existing data.
		int len = tat[offset+1]-tat[offset];
		System.arraycopy(data, tat[offset], data, floor, len);
		tat[offset] = floor;
		tat[offset+1] = floor+len;
	}
	
	//append bytes on to the front of the text after applying trim
	//may need to move existing text or previous texts
	//if there is no room after moving everything throws
	void appendHead(int idx, int trimHead, byte[] source, int sourceIdx, int sourceLen) {
		System.arraycopy(source, sourceIdx, data, makeSpaceForPrepend(idx, trimHead, sourceLen), sourceLen);			
	}
	
	void appendHead(int idx, int trimHead, ByteBuffer source, int sourceIdx, int sourceLen) {
		int targetIdx = makeSpaceForPrepend(idx, trimHead, sourceLen);
		
		int i = sourceLen;
		while (--i>=0) {
			data[targetIdx+i] = source.get(sourceIdx+i);
		}
		
	}
	
	void appendHead(int idx, byte value) {
		
		//everything is now ready to trim and copy.
		data[makeSpaceForPrepend(idx, 0, 1)] = value;
	}

	int makeSpaceForPrepend(int idx, int trimHead, int sourceLen) {
		int textLength = sourceLen-trimHead;
		if (textLength<0) {
			textLength = 0;
		}
		//if not room make room checking before first because thats where we want to copy the head.
		int offset = idx<<2;
				
		makeSpaceForPrepend(offset, textLength);
		//everything is now ready to trim and copy.
		
		//keep the max head append size
		int maxHead = tat[offset+2];
		if (textLength>maxHead) {
			tat[offset+2] = textLength;
		}
			
		//everything is now ready to trim and copy.
		int newStart = tat[offset]-textLength;
		tat[offset]=newStart;
		return newStart;
	}


	private void makeSpaceForPrepend(int offset, int textLength) {
		if (tat[offset]>tat[offset+1]) {
			//null or empty string detected so change to simple set
			tat[offset+1]=tat[offset];
		}
		
		int start = tat[offset]-textLength;
		int limit = offset-3<0 ? 0 : tat[offset-3];
				
		if (start<limit) {
			int stop = tat[offset+4];
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
	}
	
	
	///////////
	//////////
	//read access methods
	//////////
	//////////
	

	public byte getChar(int idx, int index) {
		return data[tat[idx<<2]+index];
	}
	
	public int get(int idx, byte[] target, int targetIdx) {
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

	public boolean equals(int idx, byte[] target, int targetIdx, int length) {
		
		int pos;
		int lim;
		byte[] buf;
		if (idx<0) {
			int offset = idx<<1;
			
			pos = initTat[offset];
			lim = initTat[offset+1];
			buf = initBuffer;
			
		} else {
			int offset = idx<<2;
			
			pos = tat[offset];
			lim = tat[offset+1];
			buf = data;
		}

		int len = lim-pos;
		if (len<0 && length==0) {
			return true;
		}
		
		int i = length;
		if (len != i) {
			return false;
		}
				
		while (--i>=0) {
			if (target[targetIdx+i]!=buf[pos+i]) {
				return false;
			}
		}
		return true;
	}

	
	public boolean equals(int idx, ByteBuffer target) {
	
		int targetIdx = target.position();
		int length = target.limit()-target.position();
		
		int pos;
		int lim;
		byte[] buf;
		if (idx<0) {
			int offset = idx<<1;
			
			pos = initTat[offset];
			lim = initTat[offset+1];
			buf = initBuffer;
			
		} else {
			int offset = idx<<2;
			
			pos = tat[offset];
			lim = tat[offset+1];
			buf = data;
		}

		int len = lim-pos;
		if (len<0 && length==0) {
			return true;
		}
		
		int i = length;
		if (len != i) {
			return false;
		}
				
		while (--i>=0) {
			if (target.get(targetIdx+i)!=buf[pos+i]) {
				return false;
			}
		}
		return true;
	}
	
	
	public int countHeadMatch(int idx, byte[] source, int sourceIdx, int sourceLength) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int limit = tat[offset+1]-pos;
		if (sourceLength<limit) {
			limit = sourceLength;
		}
		int i = 0;
		while (i<limit && data[pos+i]==source[sourceIdx+i]) {
			i++;
		}
		return i;
	}
	
	public int countHeadMatch(int idx, ByteBuffer source) {
		int offset = idx<<2;
		
		int sourceLength = source.remaining();
		int sourceIdx = source.position();
		
		int pos = tat[offset];
		int limit = tat[offset+1]-pos;
		if (sourceLength<limit) {
			limit = sourceLength;
		}
		int i = 0;
		while (i<limit && data[pos+i]==source.get(sourceIdx+i)) {
			i++;
		}
		return i;
	}
	
	public int countTailMatch(int idx, byte[] source, int sourceLength, int sourceLast) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		
		int limit = Math.min(sourceLength,lim-pos);
		int i = 1;
		while (i<=limit && data[lim-i]==source[sourceLast-i]) {
			i++;
		}
		return i-1;
	}
	
	public int countTailMatch(int idx, ByteBuffer source) {
		int offset = idx<<2;
		
		int sourceLength = source.remaining();
		int sourceLast = source.limit();
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		
		int limit = Math.min(sourceLength,lim-pos);
		int i = 1;
		while (i<=limit && data[lim-i]==source.get(sourceLast-i)) {
			i++;
		}
		return i-1;
	}
	
	public int itemCount() {
		return tatLength>>2;
	}
	
	public int length(int idx) {
		int result;
		if (idx<0) {
			result = initLength(idx);
		} else {
			result = valueLength(idx);
		}
		return result < 0 ? 0 : result;
	}


    public int valueLength(int idx) {
        int result;
        int offset = idx<<2;
        result = tat[offset+1] - tat[offset];
        return result;
    }


    public int initLength(int idx) {
        int result;
        int offset = idx << 1; //this shift left also removes the top bit! sweet.
        result = initTat[offset+1] - initTat[offset];
        return result;
    }

	public void copy(int sourceIdx, int targetIdx) {
		int len;
		int startFrom;
		byte[] buffer;
		if (sourceIdx<0) {
			int offset = sourceIdx << 1; //this shift left also removes the top bit! sweet.
			startFrom = initTat[offset];
			len = initTat[offset+1] - startFrom;
			buffer = initBuffer;
		} else {
			int offset = sourceIdx<<2;
			startFrom = tat[offset];
			len = tat[offset+1] - startFrom;
			buffer = data;
		}
		if (len<0) {
			setNull(targetIdx);
			return;
		}		
		set(targetIdx, buffer, startFrom, length(sourceIdx));
	}


	public int getIntoRing(int idx, int[] target, int targetIdx, int targetMask) {
		byte[] buf;
		int len;
		int pos;
		if (idx<0) {
			int offset = idx << 1; //this shift left also removes the top bit! sweet.
			
			pos = initTat[offset];
			len = initTat[offset+1]-pos;
			buf = initBuffer;
			
		} else {
		
			int offset = idx<<2;
			
			pos = tat[offset];
			len = tat[offset+1]-pos;
			buf = data;
		}
		
		int i = len;
		int j = 4;
		int v = 0;
		int k = (len+3)>>>2;
		while (--i>=0) {//NOTE: may want to unroll if this becomes a problem.			
			//int value = Integer.
			if (--j>=0) {
				v = (v<<8)|buf[pos+i];
			}
			if (j==0) {
				target[(targetMask)&(targetIdx+k)] = v;
				k--;
				j = 4;
			}	
			
		}	
		if (j!=4) {
			target[(targetMask)&(targetIdx+k)] = v;
		}
		
		return len;
	}
	
    public int copyToRingBuffer(int idx, byte[] target, final int targetIdx, final int targetMask) {
        // Does not support init values
        assert (idx > 0);

        final int offset = idx << 2;
        final int pos = tat[offset];
        final int len = tat[offset + 1] - pos;

        int tStart = targetIdx & targetMask;
        if (1 == len) {
            // simplification because 1 char can not loop around ring buffer.
            target[tStart] = data[pos];
        } else {
            copyToRingBuffer(target, targetIdx, targetMask, pos, len, tStart);
        }
        return targetIdx + len;
    }

    private void copyToRingBuffer(byte[] target, final int targetIdx, final int targetMask, final int pos,
            final int len, int tStart) {
        int tStop = (targetIdx + len) & targetMask;
        if (tStop > tStart) {
            System.arraycopy(data, pos, target, tStart, len);
        } else {
            // done as two copies
            int firstLen = targetMask - tStart;
            System.arraycopy(data, pos, target, tStart, firstLen);
            System.arraycopy(data, pos + firstLen, target, 0, len - firstLen);
        }
    }
	
	
}
