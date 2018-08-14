package com.ociweb.pronghorn.util.search;

public class KnuthMorrisPratt {



		public static void search(byte[] pat, int patOff, int patLen, int pathMask, 
								   byte[] data, int dataOffset, int dataLength, int dataMask,
				                   int[] jumps, MatchVisitor visitor) {
		    int j = 0; 
			int i = 0;
			while (i < dataLength) {
			    
				if (pat[pathMask&(patOff+j)] == data[dataMask&(dataOffset+i)]) {
			        j++;
			        i++;
			    }
			    
			    if (j == patLen) {
			    	if (!visitor.visit(i-j)) {//pattern found at this index
			    		return;//stop since visitor returned false
			    	}
			        j = jumps[j-1];
			    } else if (i < dataLength && pat[pathMask&(patOff+j)] != data[dataMask&(dataOffset+i)]) {
			        if (j != 0) {
			            j = jumps[j-1];
			        } else {
			            i = i+1;
			        }
			    }
			    
			}
		}
	 
	    public static void populateJumpTable(final byte[] pat, final int patOff, final int patLen, final int patMask,
	    		                             final int jumpTable[]) {
	        int i = 1;
	        jumpTable[0] = 0; // start at zero
	        int runLen = 0;
	        while (i < patLen) {
	            if (pat[patMask&(patOff+i)] == pat[patMask&(patOff+runLen)]) {
	                runLen++;
	                jumpTable[i] = runLen;
	                i++;
	            } else {
	                if (runLen != 0) {
	                    runLen = jumpTable[runLen-1];
	                    //do not inc i here, by design
	                } else {
	                    jumpTable[i] = runLen;
	                    i++;
	                }
	            }
	        }
	    }

	
}
