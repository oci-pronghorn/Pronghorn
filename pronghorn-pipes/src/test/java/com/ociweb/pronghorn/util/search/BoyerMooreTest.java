package com.ociweb.pronghorn.util.search;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class BoyerMooreTest {

	@Test
	public void simpleTest() {
		
		byte[] pat = "ABABCABAB".getBytes();
		byte[] txt = "ABABDABACDABABCABAB".getBytes();
		
	    final AtomicInteger count = new AtomicInteger();
		
	    if (pat.length > 0) {
		      	
		    int[] charTable = new int[BoyerMoore.ALPHABET_SIZE];	        
		    BoyerMoore.populateCharTable(pat, charTable);   
		
		    int[] offsetTable = new int[pat.length];    	
		    BoyerMoore.populateOffsetTable(pat, offsetTable);
		    
		    MatchVisitor visitor = new MatchVisitor() {
				@Override
				public boolean visit(int position) {
					count.incrementAndGet();
					assertEquals(10, position);
					return false;
				}
		    };
		    		    
			BoyerMoore.search(txt, 0, txt.length, Integer.MAX_VALUE,
							 pat, 0, pat.length, Integer.MAX_VALUE,
		    	   visitor, 
		    	   offsetTable, charTable);
				
			assertEquals(1, count.get());
			
		}
	}
	
	
}
