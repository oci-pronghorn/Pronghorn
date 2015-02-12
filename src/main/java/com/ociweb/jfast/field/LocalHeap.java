//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffer.PaddedLong;



/**
 * Arena memory pattern where we allocate a single block large enough for all the
 * dynamic fields needs when encoding or decoding.  The dynamic fields are those who's 
 * size is not always the same like ASCII/UTF-8 text or the byte vector fields.
 * 
 * Data is moved as needed to make room.  This should be rare because it is expected that
 * we have enough padding between each field to compensate for regular work.
 * 
 * Other "transformations" of these bytes may and should be done at read time.  This allows
 * decoding to skip this work and it allows the data to be stored in smaller format.  If the
 * data is never used then the translation need not be done.  Some examples are the conversion of
 * bytes to chars via the ASCII and UTF-8 encodings.
 *  
 *  
 * @author Nathan Tippy
 * 
 */
public class LocalHeap {

    
    private int totalContent = 0; // total chars consumed by current text.
    private int totalWorkspace = 0; // working space around each text.

    public final byte[] data;
    private final int gapCount;
    private final int dataLength;
    private final int tatLength;

    public final int[] initTat;
    private final byte[] initBuffer;

    // remain true unless memory gets low and it has to give up any margin
    private boolean preserveWorkspace = true;
    private final int itemCount;

    // text allocation table
    public final int[] tat;
    public static final int INIT_VALUE_MASK = 0x80000000;

    // 4 ints per each in ring buffer.
    // start index position (inclusive)
    // stop index limit (exclusive)
    // max prefix append
    // max postfix append

    public LocalHeap(int singleTextSize, int singleGapSize, int fixedTextItemCount) {
        this(singleTextSize, singleGapSize, fixedTextItemCount, 0, new int[0], new byte[0][]);
    }

    public LocalHeap(int singleTextSize, int singleGapSize, int fixedTextItemCount, int byteInitTotalLength,
            int[] byteInitIndex, byte[][] byteInitValue) {

        if (singleTextSize<1) {
            throw new UnsupportedOperationException("Text length must be 1 or more.");
        }
        
        itemCount = fixedTextItemCount;

        gapCount = fixedTextItemCount + 1;
        dataLength = (singleGapSize * gapCount) + (singleTextSize * fixedTextItemCount);
        tatLength = (fixedTextItemCount << 2);
        tat = new int[tatLength + 1];// plus 1 to get dataLength without
                                     // conditional
        tat[tatLength] = dataLength;
        initTat = new int[fixedTextItemCount << 1];

        data = new byte[dataLength];

        int i = tatLength;
        int j = dataLength + (singleTextSize >> 1);
        while (--i >= 0) {
            int idx = (j -= (singleTextSize + singleGapSize));
            i--;
            i--;
            tat[i--] = idx;
            tat[i] = idx;
        }

        if (null == byteInitValue || byteInitValue.length == 0) {
            initBuffer = null;
        } else {
            initBuffer = new byte[byteInitTotalLength];

            int stopIdx = byteInitTotalLength;
            int startIdx = stopIdx;

            i = byteInitValue.length;
            while (--i >= 0) {
            	boolean isNull = null == byteInitValue[i];
                int len = isNull ? 0 : byteInitValue[i].length;
                startIdx -= len;
                if (len > 0) {
                    System.arraycopy(byteInitValue[i], 0, initBuffer, startIdx, len);
                }
                // will be zero zero for values without constants.
                int offset = byteInitIndex[i] << 1;
                initTat[offset] = startIdx;
                initTat[offset + 1] = isNull ? startIdx-1 : stopIdx; 
                stopIdx = startIdx;
            }
        }

    }

    public static void reset(LocalHeap heap) {
        int i = heap.itemCount;
        while (--i >= 0) {
            setNull(i, heap);
        }
    }

    public static void setZeroLength(int idx, LocalHeap byteHeap) {
        int offset = idx << 2;
        byteHeap.tat[offset + 1] = byteHeap.tat[offset];
    }

    public static void setNull(int idx, LocalHeap byteHeap) {
        int offset = idx << 2;
        byteHeap.tat[offset + 1] = byteHeap.tat[offset] - 1;
    }

    public static boolean isNull(int idx, LocalHeap heap) {
        int offset = idx << 2;
        return heap.tat[offset + 1] == heap.tat[offset] - 1;
    }

    public static int allocate(int idx, int sourceLen, LocalHeap heap) {
        int offset = idx << 2;

        int target = heap.makeRoom(offset, sourceLen);

        int limit = target + sourceLen;
        if (limit > heap.dataLength) {
            target = heap.dataLength - sourceLen;
        }

        // full replace
        heap.totalContent += (sourceLen - (heap.tat[offset + 1] - heap.tat[offset]));
        heap.tat[offset] = target;
        heap.tat[offset + 1] = limit;

        return target;
    }
    
    private int makeRoom(int offset, int sourceLen) {
        int prevTextStop = offset == 0 ? 0 : tat[offset - 3];
        int nextTextStart = tat[offset + 4];
        int space = nextTextStart - prevTextStop;
        int middleIdx = (space - sourceLen) >> 1;

        if (sourceLen > space) {
            // we do not have enough space move to the bigger side.
            makeRoom(offset, middleIdx > (dataLength >> 1), (sourceLen + tat[offset + 2] + tat[offset + 3]) - space);
            prevTextStop = offset == 0 ? 0 : tat[offset - 3];
            nextTextStart = offset + 4 >= dataLength ? dataLength : tat[offset + 4];
            middleIdx = ((nextTextStart - prevTextStop) - sourceLen) >> 1;
        }

        int target = prevTextStop + middleIdx;
        return target < 0 ? 0 : target;
    }

    private void setInternal(int idx, byte[] source, int startFrom, int copyLength) {
        
        int offset = idx << 2;

        totalContent += (copyLength - (tat[offset + 1] - tat[offset]));
        int target = makeRoom(offset, copyLength);
        tat[offset] = target;
        tat[offset + 1] = target + copyLength;

        System.arraycopy(source, startFrom, data, target, copyLength);
    }
    
    public static void set(int idx, byte[] source, int startFrom, int copyLength, int sourceMask, LocalHeap heap) {
        int offset = idx << 2;

        heap.totalContent += (copyLength - (heap.tat[offset + 1] - heap.tat[offset]));
        int target = heap.makeRoom(offset, copyLength);
        heap.tat[offset] = target;
        heap.tat[offset + 1] = target + copyLength;

        int i = copyLength;
        while (--i>=0) {
            heap.data[target+i] = source[sourceMask&(startFrom+i)];
        }   
    }

    private void makeRoom(int offsetNeedingRoom, boolean startBefore, int totalDesired) {

        if (startBefore) {
            totalDesired = makeRoomBeforeFirst(offsetNeedingRoom, totalDesired);
        } else {
            totalDesired = makeRoomAfterFirst(offsetNeedingRoom, totalDesired);
        }
        if (totalDesired > 0) {
            throw new RuntimeException("LocalHeap must be initialized larger for required text of length "+totalDesired);
        }
    }

    private int makeRoomAfterFirst(int offsetNeedingRoom, int totalDesired) {
        // compress rear its larger
        totalDesired = compressAfter(offsetNeedingRoom + 4, totalDesired);
        if (totalDesired > 0) {
            totalDesired = compressBefore(offsetNeedingRoom - 4, totalDesired);
            if (totalDesired > 0) {
                preserveWorkspace = false;
                totalDesired = compressAfter(offsetNeedingRoom + 4, totalDesired);
                if (totalDesired > 0) {
                    totalDesired = compressBefore(offsetNeedingRoom - 4, totalDesired);
                }
            }
        }
        return totalDesired;
    }

    private int makeRoomBeforeFirst(int offsetNeedingRoom, int totalDesired) {
        // compress front its larger
        totalDesired = compressBefore(offsetNeedingRoom - 4, totalDesired);
        if (totalDesired > 0) {
            totalDesired = compressAfter(offsetNeedingRoom + 4, totalDesired);
            if (totalDesired > 0) {
                preserveWorkspace = false;
                totalDesired = compressBefore(offsetNeedingRoom - 4, totalDesired);
                if (totalDesired > 0) {
                    totalDesired = compressAfter(offsetNeedingRoom + 4, totalDesired);
                }
            }
        }
        return totalDesired;
    }

    private int compressBefore(int stopOffset, int totalDesired) {
        // moving everything to the left until totalDesired
        // start at zero and move everything needed if possible.
        int dataIdx = 0;
        int offset = 0;
        while (offset <= stopOffset) {

            int leftBound = 0;
            int rightBound = 0;
            int textLength = tat[offset + 1] - tat[offset];
            int totalNeed = textLength;

            if (preserveWorkspace) {
                int padding = 0;
                leftBound = tat[offset + 2];
                rightBound = tat[offset + 3];

                padding += leftBound;
                padding += tat[offset + 3];
                int avgPadding = (dataLength - (this.totalContent + this.totalWorkspace)) / gapCount;
                if (avgPadding < 0) {
                    avgPadding = 0;
                }

                padding += avgPadding;

                int halfPad = avgPadding >> 1;
                leftBound += halfPad;
                rightBound += halfPad;

                totalNeed += padding;
            }

            int copyTo = dataIdx + leftBound;

            if (copyTo < tat[offset]) {
                // copy down and save some room.
                System.arraycopy(data, tat[offset], data, copyTo, textLength);
                tat[offset] = copyTo;
                int oldStop = tat[offset + 1];
                int newStop = copyTo + textLength;
                tat[offset + 1] = newStop;

                totalDesired -= (oldStop - newStop);

                dataIdx = dataIdx + totalNeed;
            } else {
                // it is already lower than this point.
                dataIdx = tat[offset + 1] + rightBound;
            }
            offset += 4;
        }

        return totalDesired;
    }

    private int compressAfter(int stopOffset, int totalDesired) {
        // moving everything to the right until totalDesired
        // start at zero and move everything needed if possible.
        int dataIdx = dataLength;
        int offset = tatLength - 4;
        while (offset >= stopOffset) {

            int leftBound = 0;
            int rightBound = 0;
            int textLength = tat[offset + 1] - tat[offset];
            if (textLength < 0) {
                textLength = 0;
            }
            int totalNeed = textLength;

            if (preserveWorkspace) {
                int padding = 0;
                leftBound = tat[offset + 2];
                rightBound = tat[offset + 3];

                padding += leftBound;
                padding += tat[offset + 3];
                int avgPadding = (dataLength - (this.totalContent + this.totalWorkspace)) / gapCount;
                if (avgPadding < 0) {
                    avgPadding = 0;
                }

                padding += avgPadding;

                int halfPad = avgPadding >> 1;
                leftBound += halfPad;
                rightBound += halfPad;

                totalNeed += padding;
            }

            int newStart = dataIdx - (rightBound + textLength);
            if (newStart < 0) {
                newStart = 0;// will leave more on totalDesired.
            }

            if (newStart > tat[offset]) {
                // copy up and save some room.
                System.arraycopy(data, tat[offset], data, newStart, textLength);
                int oldStart = tat[offset];
                tat[offset] = newStart;
                totalDesired -= (newStart - oldStart);
                tat[offset + 1] = newStart + textLength;
                dataIdx = dataIdx - totalNeed;
            } else {
                // it is already greater than this point.
                dataIdx = tat[offset] - leftBound;
            }

            offset -= 4;
        }

        return totalDesired;
    }

       
    // append chars on to the end of the text after applying trim
    // may need to move existing text or following texts
    // if there is no room after moving everything throws
    public static void appendTail(int idx, int trimTail, byte[] source, int sourceIdx, int sourceLen, int sourceMask, LocalHeap heap) {
        // if not room make room checking after first because thats where we
        // want to copy the tail.
        
        //System.arraycopy(source, sourceIdx, data, makeSpaceForAppend(idx, trimTail, sourceLen), sourceLen);
        int targetIdx =  makeSpaceForAppend(idx,trimTail,sourceLen,heap);
        int i = sourceLen;
        while (--i>=0) {
            heap.data[targetIdx+i] = source[sourceMask&(sourceIdx+i)];            
        }        
    }
    
    // never call without calling setZeroLength first then a sequence of these
    // never call without calling offset() for first argument
    public int appendTail(int offset, int nextLimit, byte value) {

        // setZeroLength was called first so no need to check for null
        if (tat[offset + 1] >= nextLimit) {
            // System.err.println("make space for "+offset);
            makeSpaceForAppend(offset, 1);
            nextLimit = tat[offset + 4];
        }

        // everything is now ready to trim and copy.
        data[tat[offset + 1]++] = value;
        return nextLimit;
    }

    public void trimTail(int idx, int trim) {
        tat[(idx << 2) + 1] -= trim;
    }

    public void trimHead(int idx, int trim) {
        int offset = idx << 2;

        int tmp = tat[offset] + trim;
        int stp = tat[offset + 1];
        if (tmp > stp) {
            tmp = stp;
        }
        tat[offset] = tmp;

    }

    public static int makeSpaceForAppend(int idx, int trimTail, int sourceLen, LocalHeap heap) {
        int textLen = (sourceLen - trimTail);

        int offset = idx << 2;

        heap.prepForAppend(offset, textLen);
        // everything is now ready to trim and copy.

        // keep the max head append size
        int maxTail = heap.tat[offset + 3];
        int dif = (sourceLen - trimTail);
        if (dif > maxTail) {
            heap.tat[offset + 3] = dif;
        }
        // target position
        int targetPos = heap.tat[offset + 1] - trimTail;
        heap.tat[offset + 1] = targetPos + sourceLen;
        return targetPos;
    }

    private void prepForAppend(int offset, int textLen) {
        if (tat[offset] > tat[offset + 1]) {
            // switch from null to zero length
            tat[offset + 1] = tat[offset];
        }

        if (tat[offset + 1] + textLen >= tat[offset + 4]) {
            makeSpaceForAppend(offset, textLen);

        }
    }

    public void makeSpaceForAppend(int offset, int textLen) { 

        int floor = offset - 3 >= 0 ? tat[offset - 3] : 0;
        int need = tat[offset + 1] + textLen - tat[offset];

        if (need > (tat[offset + 4] - floor)) {
            makeRoom(offset, false, need);
        }
        // we have some space so just shift the existing data.
        int len = tat[offset + 1] - tat[offset];
        System.arraycopy(data, tat[offset], data, floor, len);
        tat[offset] = floor;
        tat[offset + 1] = floor + len;
    }

    // append chars on to the front of the text after applying trim
    // may need to move existing text or previous texts
    // if there is no room after moving everything throws
    public static void appendHead(int idx, int trimHead, byte[] source, int sourceIdx, int sourceLen, int sourceMask,LocalHeap heap) {
        int targetIdx = makeSpaceForPrepend(idx,trimHead,sourceLen,heap);
        int i = sourceLen;
        while (--i>=0) {
            heap.data[targetIdx+i] = source[sourceMask&(sourceIdx+i)];
        }
    }
    
    public static void appendHead(int idx, byte value, LocalHeap heap) {
        // everything is now ready to trim and copy.
        heap.data[makeSpaceForPrepend(idx,0,1,heap)] = value;
    }

    public static int makeSpaceForPrepend(int idx, int trimHead, int sourceLen, LocalHeap heap) {
    	if (sourceLen>heap.dataLength) {
    		throw new ArrayIndexOutOfBoundsException("Need to make space for "+sourceLen+" but the max size is "+heap.dataLength);
    	}
    	
        int textLength = sourceLen - trimHead;
        if (textLength < 0) {
            textLength = 0;
        }
        // if not room make room checking before first because thats where we
        // want to copy the head.
        int offset = idx << 2;

        heap.makeSpaceForPrepend(offset, textLength);
        // everything is now ready to trim and copy.

        // keep the max head append size
        int maxHead = heap.tat[offset + 2];
        if (textLength > maxHead) {
            heap.tat[offset + 2] = textLength;
        }

        // everything is now ready to trim and copy.
        int newStart = heap.tat[offset] - textLength;
        if (newStart < 0) {
            newStart = 0;
        }
        heap.tat[offset] = newStart;
        return newStart;
    }

    private void makeSpaceForPrepend(int offset, int textLength) {
        if (tat[offset] > tat[offset + 1]) {
            // null or empty string detected so change to simple set
            tat[offset + 1] = tat[offset];
        }

        int start = tat[offset] - textLength;

        if (start < 0) {
            // must move this right first.
            makeRoomBeforeFirst(offset, textLength);
            start = tat[offset] - textLength;
        }
        if (start < 0) {
            start = 0;
        }

        int limit = offset - 3 < 0 ? 0 : tat[offset - 3];

        if (start < limit) {
            int stop = tat[offset + 4];
            int space = stop - limit;
            int need = tat[offset + 1] - start;

            if (need > space) {
                makeRoom(offset, true, need);
            }
            // we have some space so just shift the existing data.
            int len = tat[offset + 1] - tat[offset];
            System.arraycopy(data, tat[offset], data, start, len);
            tat[offset] = start;
            tat[offset + 1] = start + len;

        }
    }

    // /////////
    // ////////
    // read access methods
    // ////////
    // ////////

    // for ring buffer only where the length was already known
    public static int copyToRingBuffer(int idx, byte[] target, final int targetIdx, final int targetMask, LocalHeap localHeap) {//Invoked 100's of millions of times, must be tight.
        final int offset = idx << 2;
        final int pos = localHeap.tat[offset];
        final int len = localHeap.tat[offset + 1] - pos;

        copyToRingBuffer(target, targetIdx, targetMask, pos, len, localHeap.data);
        return targetIdx + len;
    }

    public static void copyToRingBuffer(byte[] target, final int targetIdx, final int targetMask, final int pos, final int len, byte[] data) {
        int tStop = (targetIdx + len) & targetMask;
        int tStart = targetIdx & targetMask;
        if (tStop > tStart) {
            System.arraycopy(data, pos, target, tStart, len);
        } else {
            // done as two copies
            int firstLen = 1+ targetMask - tStart;
            System.arraycopy(data, pos, target, tStart, firstLen);
            System.arraycopy(data, pos + firstLen, target, 0, len - firstLen);
        }
    }

    
    public static byte byteAt(int idx, int at, LocalHeap heap) {
        if (idx < 0) {
            int offset = idx << 1; // this shift left also removes the top bit!
            return heap.initBuffer[heap.initTat[offset] + at];
        } else {
            int offset = idx << 2;
            return heap.data[heap.tat[offset] + at];
        }
    }

    public static boolean equals(int idx, byte[] target, int targetIdx, int targetLen, int targetMask, LocalHeap heap) {
         if (idx < 0) {
             int offset = idx << 1;
             return eq(target, targetIdx, targetLen, targetMask, heap.initTat[offset], heap.initTat[offset + 1], heap.initBuffer);
         } else {
             int offset = idx << 2;
             return eq(target, targetIdx, targetLen, targetMask, heap.tat[offset], heap.tat[offset + 1], heap.data);
         }
     }
        
    private static boolean eq(byte[] target, int targetIdx, int length, int targetMask, int pos, int lim, byte[] buf) {
        int len = lim - pos;
        if (len<0) {
            len = 0;
        }
        if (length<0) {
        	length = 0;
        }//TODO: D, Investigate if we need to tell the difference between absent and zero length string of bytes
        if (len != length) {
            return false;
        }
                
        if (len>0) {
            int i = length;
            while (--i >= 0) {
                if (target[targetMask&(targetIdx + i)] != buf[pos + i]) {
                    return false;
                }
            }
        }
        return true;
    }
    

    public String toString(int idx) {
        byte[] buf;
        int len;
        int pos;
        if (idx < 0) {
            int offset = idx << 1; // this shift left also removes the top bit!
                                   // sweet.

            pos = initTat[offset];
            len = initTat[offset + 1] - pos;
            buf = initBuffer;

        } else {

            int offset = idx << 2;

            pos = tat[offset];
            len = tat[offset + 1] - pos;
            buf = data;
        }

        StringBuilder builder = new StringBuilder();
        
        
        int i = len;
        while (--i >= 0) {
            builder.append(',').append(buf[pos++]);
        }

        return builder.toString();
    }
    
    public String toASCIIString(int idx) {
        byte[] buf;
        int len;
        int pos;
        if (idx < 0) {
            int offset = idx << 1; // this shift left also removes the top bit!
                                   // sweet.

            pos = initTat[offset];
            len = initTat[offset + 1] - pos;
            buf = initBuffer;

        } else {

            int offset = idx << 2;

            pos = tat[offset];
            len = tat[offset + 1] - pos;
            buf = data;
        }

        StringBuilder builder = new StringBuilder();
        
        
        int i = len;
        while (--i >= 0) {
            builder.append((char)buf[pos++]);
        }

        return builder.toString();
    }
    
    public static int countHeadMatch(int idx, byte[] source, int sourceIdx, int sourceLength, int sourceMask, LocalHeap heap) {
        int offset = idx << 2;

        int pos = heap.tat[offset];
        int limit = heap.tat[offset + 1] - pos;
        if (sourceLength < limit) {
            limit = sourceLength;
        }
        int i = 0;
        while (i < limit && heap.data[pos + i] == source[sourceMask & (sourceIdx + i)]) {
            i++;
        }
        return i;
    }

    public static int countTailMatch(int idx, byte[] source, int endingPos, int sourceLength, int sourceMask, LocalHeap heap) {
        int offset = idx << 2;

        int pos = heap.tat[offset];
        int lim = heap.tat[offset + 1];

        int limit = Math.min(sourceLength, lim - pos);
        int i = 1;
        while (i <= limit && heap.data[lim - i] == source[sourceMask & (endingPos - i)]) {
            i++;
        }
        return i - 1;
    }

    public static int itemCount(LocalHeap heap) {
        return heap.tatLength >> 2;
    }

    public static int initStartOffset(int idx, LocalHeap heap) {
        return heap.initTat[idx << 1];
    }

    public static int length(int idx, LocalHeap heap) {
        int result = (idx < 0 ? LocalHeap.initLength(idx, heap) : LocalHeap.valueLength(idx,heap));
        return result < 0 ? 0 : result;
    }

    public static int valueLength(int idx, LocalHeap heap) {
        int offset = idx << 2;
        return heap.tat[offset + 1] - heap.tat[offset];
    }

    public static int valueLength(int idx, int[] tat) {
        int offset = idx << 2;
        return tat[offset + 1] - tat[offset];
    }

    public static int initLength(int idx, LocalHeap heap) {
        int offset = idx << 1; // this shift left also removes the top bit!
                               // sweet.
        return heap.initTat[offset + 1] - heap.initTat[offset];
    }
    
    public static int initLength(int idx, int[] initTat) {
        int offset = idx << 1; // this shift left also removes the top bit!
                               // sweet.
        return initTat[offset + 1] - initTat[offset];
    }

    public static void copy(int sourceIdx, int targetIdx, LocalHeap heap) {
        int len;
        int startFrom;
        byte[] buffer;
        if (sourceIdx < 0) {
            int offset = sourceIdx << 1; // this shift left also removes the top
                                         // bit! sweet.
            startFrom = heap.initTat[offset];
            len = heap.initTat[offset + 1] - startFrom;
            buffer = heap.initBuffer;
        } else {
            int offset = sourceIdx << 2;
            startFrom = heap.tat[offset];
            len = heap.tat[offset + 1] - startFrom;
            buffer = heap.data;
        }
        if (len < 0) {
            setNull(targetIdx, heap);
            return;
        }
        heap.setInternal(targetIdx, buffer, startFrom, LocalHeap.length(sourceIdx,heap));
    }

    private int start(int offset) {
        return tat[offset];
    }

    private int stop(int offset) {
        return tat[offset + 1];
    }

    public static void addLocalHeapValue(int heapId, int sourceLen, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
	    final int p = rbRingBuffer.byteWorkingHeadPos.value;
	    if (sourceLen > 0) {
	        final int offset = heapId << 2;
			final int pos = byteHeap.tat[offset];
			final int len = byteHeap.tat[offset + 1] - pos;
			
			copyToRingBuffer(rbRingBuffer.byteBuffer, p, rbRingBuffer.byteMask, pos, len, byteHeap.data);
			rbRingBuffer.byteWorkingHeadPos.value = p + len;
	    }      
	    
	    RingBuffer.addBytePosAndLen(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, RingBuffer.bytesWriteBase(rbRingBuffer), p, sourceLen);
	}

	public static void reset(int idx, LocalHeap heap) {
        int offset = idx << 1; // this shift left also removes the top bit!
                               // sweet.
        int startFrom = heap.initTat[offset];
        int len = heap.initTat[offset + 1] - startFrom;
        heap.setInternal(idx, heap.initBuffer, startFrom, len);

    }

    public static void setSingleByte(byte b, int idx, LocalHeap byteHeap) {
        // This implementation assumes that all text can always support length of 1. Enforced in constructor.
        final int offset = idx << 2;
                
        int targIndex = byteHeap.tat[offset]; // because we have zero length

        byteHeap.data[targIndex] = b;
        byteHeap.tat[offset + 1] = 1 + targIndex;
    }

    public static byte[] rawAccess(LocalHeap heap) {
        return heap.data;
    }
    
    public static byte[] rawInitAccess(LocalHeap heap) {
        return heap.initBuffer;
    }
    
    
}
