package com.ociweb.pronghorn.util.primitive;

import com.ociweb.pronghorn.pipe.util.IntArrayPool;
import com.ociweb.pronghorn.pipe.util.IntArrayPoolLocal;

public class LoisOpSimpleList extends LoisOperator {

	private final int id;
	
	//     no block can be larger than 1<<29+2
	private static final int maxBlockSize = (1<<29)+2;
	private static final int maskCount = (1<<29)-1;
	
	//first int is the next jump
	//second int top 3 has operator id
	//second int lower 29 has count of entries
	
	public LoisOpSimpleList(int id) {
		this.id = id;
	}

	private final int getCount(int idx, Lois lois) {
		assert((lois.data[idx+1]&maskCount) <= lois.blockSize) : "Bad count value found";		
		return lois.data[idx+1]&maskCount;
	}
	
	private static final void setCount(int idx, int count, Lois lois) {
		if (count > maxBlockSize) {
			throw new RuntimeException("Bad count "+count);
		}		
		
		assert(count <= lois.blockSize) : "Count is larger than allowed max for this block";
		lois.data[idx+1] =   (count&maskCount) | (Lois.LOISOpSimpleList<<29);
		
	}

	@Override
	public boolean isBefore(int idx, int value, Lois lois) {
		assert(lois.blockSize < maxBlockSize);
		return 0==getCount(idx, lois) ? true : value < lois.data[idx+2];
	}

	@Override
	public boolean isAfter(int idx, int value, Lois lois) {
		assert(lois.blockSize < maxBlockSize);
		int count = getCount(idx, lois);
		return 0==count ? false : value > lois.data[idx+2+count-1];
	}

	@Override
	public boolean remove(int prev, int idx, int value, Lois lois) {
		assert(lois.blockSize < maxBlockSize);

		int count = getCount(idx, lois);
		int pos = idx+2;
		int limit = count+pos;
		for(int i=pos; i<limit; i++) {
			if (lois.data[i]==value) {
				
				int length = (limit-i)-1;
				if (length>0) {
					System.arraycopy(lois.data, i+1, lois.data, i, length);
				}
				setCount(idx, count-1, lois);
				
				//never remove the root.
				if (count==1 && prev!=-1) {
					//we removed last one
					lois.data[prev] = lois.data[idx];//previous points to next
					lois.recycle(idx);
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean insert(int idx, int value, Lois lois) {
		assert(lois.blockSize < maxBlockSize);

		if (getCount(idx, lois)<(lois.blockSize-2)) {

			//we have room to add one more
			//insert into the right place
			return insertInOrder(idx, value, lois);
			
		} else {
			int count = getCount(idx, lois);
			int pos = idx+2;
			int limit = count+pos;

			if (lois.supportBitMaps) {
				//if range used here is smaller than the bit mask then do the conversion
				//System.err.println("first "+lois.data[pos]+" last "+lois.data[limit-1]);
				if ( Math.abs(lois.data[limit-1] - lois.data[pos]) < LoisOpBitMap.valuesTracked(lois)) {
					//change this block to use bit mask
					
					IntArrayPool that = IntArrayPoolLocal.get();
					int id = IntArrayPool.lockInstance(that, count);
					int[] valuesToKeep = IntArrayPool.getArray(that, count, id);
					System.arraycopy(lois.data, pos, valuesToKeep, 0, count);
					
					//System.err.println("values to keep "+Arrays.toString(valuesToKeep));

					
					//the above array holds the values so we can write over this block.
					
					//bitmap method to format this block the right way.
					LoisOpBitMap.reviseBlock(idx, valuesToKeep, Math.min(value, valuesToKeep[0]), lois);
					
					
					//release temp space now that we are done
					IntArrayPool.releaseLock(that, count, id);
					
					//now add the new value..
					return lois.operator(idx).insert(idx, value, lois);
				}
			}

			//if we can't convert to the more efficient bit map then
			//we will have to split this into two blocks based on 
			//where we find the biggest gap
			final int maxGapIdx = maxGapIdx(lois, pos, limit);
			////////////////////////////	

			return splitOnGapAndInsert(idx, value, count, limit, maxGapIdx, lois);
		}
	}

	private boolean splitOnGapAndInsert(int idx, int value, int count, int limit, final int maxGapIdx,
			Lois lois) {

		//new next
		int newBlockId = createNewBlock(maxGapIdx, idx, limit, lois);
		
		//trim current block
		setCount(idx, count-(limit-maxGapIdx), lois);
		lois.data[idx] = newBlockId; 
		
		//now insert new value
		
		if (lois.data[maxGapIdx] > value) {
			//insert here

			return insertInOrder(idx, value, lois);				
		} else {
			//insert in new block

			return insertInOrder(lois.data[idx], value, lois);				
		}
	}

	private boolean insertInOrder(int idx, int value, Lois lois) {
		int count = getCount(idx, lois);
		int pos = idx+2;
		final int limit = count+pos;

		for(int i=pos; i<limit; i++) {			
			int temp = lois.data[i];
			if (temp>=value) {
				if (temp>value) {
					//insert						
					System.arraycopy(lois.data, i, lois.data, i+1, limit-i);
					lois.data[i] = value;						
					setCount(idx, count+1, lois);
					return true;
				} else {
					//already inserted
					return false;
				}
			}
		}

		//must add on the end
		lois.data[limit] = value;
		setCount(idx, count+1, lois);

		return true;
	}

	private int createNewBlock(int maxGapIdx, int idx, int limit, Lois lois) {
		
		int newBlockId = lois.newBlock();
		
		int count = limit-maxGapIdx;
		assert(count>=0) : "length "+count+" gap start "+maxGapIdx+" limit "+limit;
		
		lois.data[newBlockId] = lois.data[idx]; //tie in next		
		setCount(newBlockId, count, lois);		
		System.arraycopy(lois.data, maxGapIdx, lois.data, newBlockId+2, count);

		
		return newBlockId;
	}

	public static int createNewBlock(int idx, Lois lois, int anchor) {
		
		int newBlockId = lois.newBlock();		
		formatNewBlock(lois, anchor, newBlockId, lois.data[idx]);		
		return newBlockId;
		
	}

	static void formatNewBlock(Lois lois, int anchor, int targetId, int nextId) {
		lois.data[targetId] = nextId; //tie in next		
		setCount(targetId, 1, lois);	
		lois.data[targetId+2] = anchor;
	}
	
	
	
	private int maxGapIdx(Lois lois, int pos, int limit) {
		int maxGapIdx = -1;
		{
			int maxGap = -1;
			int previous = lois.data[pos];
			for(int i=pos; i<limit; i++) {			
				int temp = lois.data[i];
				int dif = temp-previous;
				if(dif >= maxGap) {
					maxGap = dif;
					maxGapIdx = i; //this is the first item to be moved
				}
				previous = temp;
			}
			assert(maxGapIdx>0) : "Should not happen because there must be a gap";
		}
		return maxGapIdx;
	}

	@Override
	public boolean visit(int idx, LoisVisitor visitor, Lois lois) {
		assert(lois.blockSize < maxBlockSize);

		int count = getCount(idx, lois);
		int pos = idx+2;
		int limit = count+pos;
		for(int i=pos; i<limit; i++) {			
			if (!visitor.visit(lois.data[i])) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean containsAny(int idx, int startValue, int endValue, Lois lois) {
		
		assert(lois.blockSize < maxBlockSize);

		int count = getCount(idx, lois);
		final int pos = idx+2;
		final int limit = count+pos;
		for(int i=pos; i<limit; i++) {		
			int value = lois.data[i];
			if (value>=startValue) {
				if (value<endValue) {
					return true;
				} else {

					
					return false;
				}
			}
		}

		return false;
	}


}
