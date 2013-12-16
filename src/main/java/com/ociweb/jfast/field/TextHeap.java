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
	
	//remain true unless memory gets low and it has to give up any margin
	private boolean preserveWorkspace = true;
	
	//text allocation table
	private final int[] tat;
	//4 ints per text body.
	//start index position.
	//stop index limit (exclusive)
	//max prefix append
	//max postfix append
	
	
	public TextHeap(int singleTextSize, int singleGapSize, int fixedTextItemCount) {
		gapCount = fixedTextItemCount+1;
		data = new char[(singleGapSize*gapCount)+(singleTextSize*fixedTextItemCount)];
		tat = new int[fixedTextItemCount<<2];
		
		int i = tat.length;
		int j = data.length+(singleTextSize>>1);
		while (--i>=0) {
			int idx = (j-=(singleTextSize+singleGapSize));
			i--;
			i--;
			tat[i--]=idx;
			tat[i]=idx;		
		}	
	}
	
	//simple replacement of last value
	//since the old value is tossed this opportunity is taken to re-center the text
	//between the one in front and the one behind.
	//if this is large it may need to move surrounding text and 
	//may throw if there is no more room in the heap.
	public void set(int idx, char[] source, int sourceIdx, int sourceLen) {
		
		int offset = idx<<2;
		
		int prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
		int nextTextStart = offset+4 >= tat.length  ? data.length  : tat[offset+4];
		int space = nextTextStart - prevTextStop;
		int middleIdx = (space-sourceLen)>>1;
		
		if (sourceLen<=space) {
			simpleReplace(source, sourceIdx, sourceLen, offset, prevTextStop, middleIdx);
		} else {
			//we do not have enough space move to the bigger side.
			makeRoom(offset, middleIdx>(data.length>>1), (sourceLen + tat[offset+2] + tat[offset+3])-space);
            
    		prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
    		nextTextStart = offset+4 >= data.length ? data.length : tat[offset+4];
    		
            simpleReplace(source, sourceIdx, sourceLen, offset, prevTextStop, ((nextTextStart-prevTextStop)-sourceLen)>>1);         
		}
	}

	public void set(int idx, CharSequence charSequence) {
		
		int offset = idx<<2;
		
		int sourceLen = charSequence.length();
		int prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
		int nextTextStart = offset+4 >= tat.length  ? data.length  : tat[offset+4];
		int space = nextTextStart - prevTextStop;
		int middleIdx = (space-sourceLen)>>1;
		
		if (sourceLen<=space) {
			simpleReplace(charSequence, offset, prevTextStop, middleIdx);
		} else {
			//we do not have enough space move to the bigger side.
			makeRoom(offset, middleIdx>(data.length>>1), (sourceLen + tat[offset+2] + tat[offset+3])-space);
            
    		prevTextStop  = offset   == 0           ? 0           : tat[offset-3];
    		nextTextStart = offset+4 >= data.length ? data.length : tat[offset+4];
    		
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
		if (limit>data.length) {
			target = data.length-sourceLen;
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
		if (limit>data.length) {
			target = data.length-charSequence.length();
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
				int avgPadding = (data.length - (this.totalContent+this.totalWorkspace))/gapCount;
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
		int dataIdx = data.length;
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
				int avgPadding = (data.length - (this.totalContent+this.totalWorkspace))/gapCount;
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
		while (i<data.length) {
			if (data[i]==0) {
				target.append('_');
			} else {
				target.append(data[i]);
			}
			i++;			
		}
		target.append(']');
		
	}


	//append chars on to the end of the text after applying trim
	//may need to move existing text or following texts
	//if there is no room after moving everything throws
	public void appendTail(int idx, int trimTail, char[] source, int sourceIdx, int sourceLen) {
		//if not room make room checking after first because thats where we want to copy the tail.
		int offset = idx<<2;
		
		int stop = tat[offset+1]+(sourceLen-trimTail);
		int limit = offset+4<tat.length ? tat[offset+4] : data.length;
		
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
	public void appendHead(int idx, int trimHead, char[] source, int sourceIdx, int sourceLen) {
		//if not room make room checking before first because thats where we want to copy the head.
		int offset = idx<<2;
		
		int start = tat[offset]-(sourceLen-trimHead);
		int limit = offset-3<0 ? 0 : tat[offset-3];
		
		
		if (start<limit) {
			int stop = offset+4<tat.length ? tat[offset+4] : data.length;
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
	
	public void get(int idx, char[] target, int targetIdx) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int len = tat[offset+1]-pos;
		
		System.arraycopy(data, pos, target, targetIdx, len);
	}
	
	public void get(int idx, Appendable target) {
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
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		
		return lim-pos;
	}
}
