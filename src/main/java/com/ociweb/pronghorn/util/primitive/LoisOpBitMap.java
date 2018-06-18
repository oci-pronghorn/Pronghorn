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

	private final int firstValue(int idx, Lois lois) {
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
			lois.data[byteOffset] |= mask;//set this bit
			return true;			
		} else {
			//already inserted			
			return false;
		}
	}

	@Override
	public boolean visit(int idx, LoisVisitor visitor, Lois lois) {
		
		int value = firstValue(idx, lois);		

		int z = idx+1;
		int dat = lois.data[z];
		for(int x = 0; x < 29; x++) {
			
			if (0!=(dat & (1<<x))) {
				if (!visitor.visit(value)) {
					return false;
				}
			} 
			value++;
		}
		
		int ints = lois.blockSize-3;
		while(--ints >= 0) {
			z++;
			dat = lois.data[z];
			
			for(int x = 0; x < 32; x++) {
				
				if (0!=(dat & (1<<x))) {
					if (!visitor.visit(value)) {
						return false;
					}
				} 
				value++;
			}
		}
		return true;
	}

	static void reviseBlock(int idx, int[] valuesToKeep, Lois lois) {
		//next will remain the same, clear the rest
		Arrays.fill(lois.data, idx+1, idx+lois.blockSize, 0);
		
		lois.data[idx+1] = (Lois.LOISOpBitMap<<29);		
		int baseValue = valuesToKeep[0];
		lois.data[idx+lois.blockSize-1] = baseValue;
		
		for(int i = 0; i<valuesToKeep.length; i++) {
			int bit = valuesToKeep[i]-baseValue;
			
			if (bit>=29) {
				final int byteOffset = (bit-29)>>5;
				final int bitOffset = (bit-29)&0x1F;
		        lois.data[idx+2+byteOffset] |= bitOffset;
			} else {
				lois.data[idx+1] |= (1<<bit);
			}
		}
		assert(lois.operator(idx)==Lois.operatorIndex[Lois.LOISOpBitMap]);
	}

	@Override
	public boolean containsAny(int idx, int startValue, int endValue, Lois lois) {
		
		int totalBits = endValue-startValue;
		
		// TODO Auto-generated method stub
		
		int firstValue = firstValue(idx, lois);
		if (firstValue < startValue) {
			//limit the range to what we have here
			totalBits -= (startValue-firstValue);
			startValue = firstValue;
		} 
				
		//what bit is the start up to end and check them
		final int bitIdx = startValue-firstValue;	    	    
	    for(int b = bitIdx; b<bitIdx+totalBits; b++) {
	    	int byteOffset = 0;
	    	int bitOffset = 0;
	    	if (b>29) {
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
