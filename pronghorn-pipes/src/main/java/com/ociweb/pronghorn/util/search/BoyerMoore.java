package com.ociweb.pronghorn.util.search;

public class BoyerMoore {


    public static void search(byte[] data, int dataOffset, int dataLength, int dataMask,
    		                  byte[] pat,  int patOffset, int patLength, int patMask,
    		                  MatchVisitor visitor, 
    		                  int[] offsetTable, int[] charTable) {
		final int limit = patLength - 1;
		for (int i = limit, j; i < dataLength;) {
		    for (j = limit; pat[patMask&(patOffset+j)] == data[dataMask&(dataOffset+i)]; --i, --j) {
		        if (j == 0) {
		        	if (!visitor.visit(i)) {
		        		return;//stop looking
		        	}
		        	break;
		        }
		    }
		    // i += needle.length - j; // to enable the naive method
		    i += Math.max(offsetTable[limit - j], charTable[data[dataMask&(dataOffset+i)]]);
		}
	}
    
    public static final int ALPHABET_SIZE = Character.MAX_VALUE + 1; // 65536
    
    
    public static void populateCharTable(final byte[] pat, final int[] table) {
       
    	int i =table.length;
    	while (--i>=0) {
    		table[i] = pat.length;    		
    	}
    	
    	final int limit = pat.length-1;
        for (i = 0; i < limit; ++i) {
            table[pat[i]] = limit - i;
        }
        
    }
        
    public static void populateOffsetTable(byte[] pat, int[] table) {

        int lastPrefixPosition = pat.length;
        for (int i = pat.length; i > 0; --i) {
            if (isPrefix(pat, i)) {
                lastPrefixPosition = i;
            }
            table[pat.length - i] = lastPrefixPosition - i + pat.length;
        }
        for (int i = 0; i < pat.length - 1; ++i) {
            final int slen = suffixLength(pat, i, 0);
            table[slen] = pat.length - 1 - i + slen;
        }
    }
    
    private static boolean isPrefix(final byte[] pat,final int p) {
        for (int i = p, j = 0; i < pat.length; ++i, ++j) {
            if (pat[i] != pat[j]) {
                return false;
            }
        }
        return true;
    }
    
    private static int suffixLength(final byte[] pat, final int p, int len) {
        for (int i = p, j = pat.length - 1;
             i >= 0 && pat[i] == pat[j];
        	 --i, --j) {
            len += 1;
        }
        return len;
    }
    
}
