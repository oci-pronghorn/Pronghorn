package com.ociweb.pronghorn.util.primitive;

import java.util.Arrays;

public class LoisOpBitMap extends LoisOperator {

	private final int id;
		
	//first int is jump to next
	//second int top 3 is the id
	//second int lower 29 are bits
	//last int is the starting position
	
	public LoisOpBitMap(int id) {
		this.id = id;
	}

	private static final int firstValue(int idx, Lois lois) {
		return lois.data[idx+lois.blockSize-1];
	}
	
	public static int valuesTracked(Lois lois) {
		return ((lois.blockSize-2)*32)-3;  //((16-2)*32)-3 -> 445		
	}
	
	private final int lastValue(int idx, Lois lois) {
		return firstValue(idx,lois)+valuesTracked(lois)-1;
	}
	
	@Override
	public boolean isBefore(int idx, int value, Lois lois) {
		return value<firstValue(idx,lois);
	}

	@Override
	public boolean isAfter(int idx, int value, Lois lois) {
		return value>lastValue(idx,lois);
	}
 
	@Override
	public boolean remove(int prev, int idx, int value, Lois lois) {
		
	    int bitIdx = value-firstValue(idx, lois);
	    
	    int byteOffset = 0;
	    int bitOffset = 0;
		if (bitIdx>29) {
			byteOffset = idx+2+((bitIdx-29)>>5);	
			bitOffset = (bitIdx-29)&0x1F;
		} else {
			byteOffset = idx+1;
			bitOffset = bitIdx;
		}
		int mask = 1<<bitOffset;
		
		if (0 == ( lois.data[byteOffset] & mask )) {
			//already gone
			return false;			
		} else {
			////System.err.println("remove bit at "+byteOffset);
			lois.data[byteOffset] ^= mask;//xor to remove this bit			
			return true;
		}
	}

	@Override
	public boolean insert(int idx, int value, Lois lois) {
		
		//if (lois.supportRLE) {
			//is this holding something which can be RLE?.. //TODO: add later?
			//find longest run is > 4 ??
		//}
		
		int firstValue = firstValue(idx, lois);
		int tracked = valuesTracked(lois);
		//System.err.println("insert "+value+"  first "+firstValue+" tracked "+tracked);
		
		if (value >= firstValue+tracked) {

			//System.err.println("new block");
			//insert a new next block, after idx.		
			int newBlockId = LoisOpSimpleList.createNewBlock(idx, lois, value);			
			lois.data[idx] = newBlockId;//do not inline data array may be modified.
			return true;
		} else {		
		
			assert(!isAfter(idx, value, lois)) : "Must not be AFTER this block must be inside it";
			if (isBefore(idx, value, lois)) {
				//we need to copy this full block as is to a new location
				//then rewrite this position as a simple pointing to new position.
		
					int newHome = lois.newBlock();	
					System.arraycopy(lois.data, idx, lois.data, newHome, lois.blockSize);	
					assert(firstValue(idx, lois) == firstValue(newHome, lois));
									
					LoisOpSimpleList.formatNewBlock(lois, value, idx, newHome);

				return false;
			} else {

				int bitIdx = value - firstValue;
			    //System.err.println("bit idx "+bitIdx);
			    int byteOffset = 0;
			    int bitOffset = 0;
				if (bitIdx >= 29) { 
					byteOffset = idx+2+((bitIdx-29)>>5);	
					bitOffset = (bitIdx-29)&0x1F;
					
					//System.err.println("bit offset "+bitOffset);
					
				} else {
					
					//0-28
					byteOffset = idx+1;
					bitOffset = bitIdx;
				}
				int mask = 1<<bitOffset;
		
				if (0 == ( lois.data[byteOffset] & mask )) {
					lois.data[byteOffset] |= mask;//set this bit
					return true;			
				} else {
					//already inserted			
					return false;
				}
			}
		}
		
	}

	@Override
	public boolean visit(int idx, LoisVisitor visitor, Lois lois) {
		
		int value = firstValue(idx, lois);		

		int z = idx+1;
		int dat = lois.data[z];
		for(int x = 0; x < 29; x++) {
			
			//System.err.println("#"+value);
			if (0 != (dat & (1<<x))) {
				//System.err.println("visit "+value);
				if (!visitor.visit(value)) {
					return false;
				}
			} 
			value++;
		}
		
		int ints = lois.blockSize-3;
		while(--ints >= 0) {
			
			dat = lois.data[++z];
			
			for(int x = 0; x < 32; x++) {
				
				//System.err.println("#"+value);
				if (0 != (dat & (1<<x))) {
					//System.err.println("visit "+value+" at bit "+x+" in byte "+(z-(idx+1)));
					
					if (!visitor.visit(value)) {
						return false;
					}
				} 
				value++;
			}
		}
		return true;
	}

	static void reviseBlock(int idx, int[] valuesToKeep, int baseValue, Lois lois) {
		//System.err.println("revise values "+Arrays.toString(valuesToKeep));
		
		//next will remain the same, clear the rest
		Arrays.fill(lois.data, idx+1, idx+lois.blockSize, 0);
		
		lois.data[idx+1] = (Lois.LOISOpBitMap<<29);		

		lois.data[idx+lois.blockSize-1] = baseValue;
		
		for(int i = 0; i<valuesToKeep.length; i++) {
			int bit = valuesToKeep[i]-baseValue;
			////System.err.println("base value "+baseValue+" values to keep "+valuesToKeep[i]+" dif "+bit);
			
			if (bit>=29) {
				final int byteOffset = (bit-29)>>5;
				final int bitOffset = (bit-29)&0x1F;
				//System.err.println("a - set bit at "+(idx+2+byteOffset));
				
		        lois.data[idx+2+byteOffset] |= (1<<bitOffset);
			} else {
				//0 - 28
				lois.data[idx+1] |= (1<<bit);
				
				//System.err.println("b - set bit at "+(idx+1));
				
			}
		}
		assert(lois.operator(idx)==Lois.operatorIndex[Lois.LOISOpBitMap]);
		assert(firstValue(idx,lois) == baseValue);

	}

	@Override
	public boolean containsAny(int idx, int startValue, int endValue, Lois lois) {
		
		int totalBits = endValue-startValue;
	
		int firstValue = firstValue(idx, lois);
		
				
		//what bit is the start up to end and check them
		final int bitIdx = startValue-firstValue;	    	
		//System.err.println("looking for "+bitIdx+" for run of "+totalBits);
		
	    for(int b = bitIdx; b<bitIdx+totalBits; b++) {
	    	int byteOffset = 0;
	    	int bitOffset = 0;
	    	if (b>=29) {
	    		byteOffset = idx+2+((b-29)>>5);	
	    		bitOffset = (b-29)&0x1F;
	    	} else {
	    		byteOffset = idx+1;
	    		bitOffset = b;
	    	}
	    	int mask = 1<<bitOffset;
	    	if ((lois.data[byteOffset]&mask) != 0) {
	    		return true;
	    	}
	    	//TODO: can we check just full ints... This would be quicker.	    	
	    }
		return false;
	}

}
