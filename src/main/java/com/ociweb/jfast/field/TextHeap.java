package com.ociweb.jfast.field;

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
	
	//text allocation table
	private final int[] tat;
	//4 ints per text body.
	//start index position.
	//stop index limit (exclusive)
	//max prefix append
	//max postfix append
	
	
	public TextHeap(int maxiNominalTextSize, int fixedTextItemCount) {
		gapCount = fixedTextItemCount+1;
		data = new char[maxiNominalTextSize*gapCount];
		tat = new int[fixedTextItemCount<<1];
		//each string must be equal distance apart for growth at the front or back
		int stepSize = data.length/gapCount;
		
		int i = tat.length;
		int j = data.length;
		while (--i>=0) {
			int idx = (j-=stepSize);
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
		
		int prevTextStop  = offset   == 0           ? 0           : tat[offset-1];
		int nextTextStart = offset+4 >= data.length ? data.length : tat[offset+4];
		int space = nextTextStart-prevTextStop;
		int middleIdx = (space-sourceLen)>>1;
		
		if (sourceLen<=space) {
			simpleReplace(source, sourceIdx, sourceLen, offset, prevTextStop, middleIdx);
		} else {
			//we do not have enough space move to the bigger side.
			makeRoom(offset, middleIdx>(data.length>>1), (sourceLen + tat[offset+2] + tat[offset+3])-space);
            
    		prevTextStop  = offset   == 0           ? 0           : tat[offset-1];
    		nextTextStart = offset+4 >= data.length ? data.length : tat[offset+4];
            simpleReplace(source, sourceIdx, sourceLen, offset, prevTextStop, ((nextTextStart-prevTextStop)-sourceLen)>>1);         
		}
	}

	private void makeRoom(int offsetNeedingRoom, boolean startBefore, int totalDesired) throws OutOfMemoryError {

		if (startBefore) {
			totalDesired = makeRoomBeforeFirst(offsetNeedingRoom, totalDesired);			
		} else {
			totalDesired = makeRoomAfterFirst(offsetNeedingRoom, totalDesired);
		}
		if (totalDesired>0) {
			throw new OutOfMemoryError("TextHeap must be initialized with more ram for required text");
		}
	}

	private int makeRoomAfterFirst(int offsetNeedingRoom, int totalDesired) {
		boolean preserveWorkspace = true;
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
		boolean preserveWorkspace = true;
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
		System.arraycopy(source, sourceIdx, data, target, sourceLen);
		//full replace
		totalContent+=(sourceLen-(tat[offset+1]-tat[offset]));
		tat[offset] = target;
		tat[offset+1] = target+sourceLen;
	}
	
	private int compressBefore(int offset, int totalDesired, boolean preserveWorkspace) {
		//moving everything to the left until totalDesired
		//start at zero and move everything needed if possible.
		int dataIdx = 0;
		int i = 0;
		while (i<offset && totalDesired>0) {
			
			int required = tat[offset+1]-tat[offset];
			int padding = 0;
			int leftBound = 0;
			
			if (preserveWorkspace) {
				leftBound = tat[offset+2]; 
				
				padding += leftBound;
				padding += tat[offset+3];
				int avgPadding = (data.length - (this.totalContent+this.totalWorkspace))/gapCount;
				padding += avgPadding;
				
				leftBound += (avgPadding>>1);
			}
			
			int totalNeed = required + padding;
			
			
			int copyTo = dataIdx+leftBound;
			
			
			
			
			//
			
			//TODO: must compute, the surrounding spacing.
			
			
			
			
			
			
			//System.arraycopy(data, tat[offset], data, dataIdx, length);
			
			
			
			//TODO: move each over until we get enough space
			
			
			
			i+=4;
		}
	
		return totalDesired;
	}

	private int compressAfter(int offset, int totalDesired, boolean preserveWorkspace) {
		//moving everything to the right until totalDesired
		//start at zero and move everything needed if possible.
		int i = data.length-4;
		while (i>offset && totalDesired>0) {
			
			//TODO: move each over until we get enough space
			
			//working space includes pre/post and average gap between each.
			
			
			i-=4;
		}
	
		return totalDesired;
	}



	//append chars on to the end of the text after applying trim
	//may need to move existing text or following texts
	//if there is no room after moving everything throws
	public void appendTail(int idx, int trimTail, char[] source, int sourceIdx, int sourceLen) {
		//if not room make room checking after first because thats where we want to copy the tail.
		int offset = idx<<2;
		
		int stop = tat[offset+1]+(sourceLen-trimTail);
		if (stop>=data.length || stop>=tat[offset+4]) {
			//must make room and may need to shift root to the left.
			
			throw new UnsupportedOperationException("no support for move yet.");
			//TODO: something
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
		if (start<0 || start<tat[offset-3]) {
			//must make room and may need to shift root to the right.
			
			throw new UnsupportedOperationException("no support for move yet.");
			//TODO: something	
		}
		
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
	
	private void get(int idx, char[] target, int targetIdx) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int len = tat[offset+1]-pos;
		
		System.arraycopy(data, pos, target, targetIdx, len);
	}
	
	private void get(int idx, StringBuilder target) {
		int offset = idx<<2;
		
		int pos = tat[offset];
		int lim = tat[offset+1];
		
		while (pos<lim) {
			target.append(data[pos++]);
		}
	}
}
