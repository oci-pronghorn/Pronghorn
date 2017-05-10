package com.ociweb.pronghorn.stage.network;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.util.TrieParser;

public class HeadersTrieTest {

	

	@Ignore
	public void testNow() {
		
		TrieParser headerMap = new TrieParser(1024,true);
		
		HTTPHeaderDefaults[] shr =  HTTPHeaderDefaults.values();
		int x = shr.length;
		
		final int END_OF_HEADER_ID = x+1;
		final int UNKNOWN_HEADER_ID = x+2;
		

		String lastHeaderMap = "";
		String newHeaderMap = null;
		
		headerMap.setUTF8Value("%b: %b\n", UNKNOWN_HEADER_ID); //TODO: bug in trie if we attemp to set this first...
		
		newHeaderMap = headerMap.toString();
		assertTrue("After adding value we expect the map to have changed but was \n"+newHeaderMap,!lastHeaderMap.equals(newHeaderMap));
		lastHeaderMap = newHeaderMap;
		
		headerMap.setUTF8Value("%b: %b\r\n", UNKNOWN_HEADER_ID);   

		newHeaderMap = headerMap.toString();
		assertTrue("After adding value we expect the map to have changed but was \n"+newHeaderMap,!lastHeaderMap.equals(newHeaderMap));
		lastHeaderMap = newHeaderMap;
		
        while (--x >= 0) {
            //must have tail because the first char of the tail is required for the stop byte
            headerMap.setUTF8Value(shr[x].readingTemplate(), "\n",shr[x].ordinal());
            headerMap.setUTF8Value(shr[x].readingTemplate(), "\r\n",shr[x].ordinal());
            
            newHeaderMap = headerMap.toString();
            assertTrue("After adding value we expect the map to have changed but was \n"+newHeaderMap,!lastHeaderMap.equals(newHeaderMap));
            lastHeaderMap = newHeaderMap;
            
        }     
        
		headerMap.setUTF8Value("\r\n", END_OF_HEADER_ID);		
        headerMap.setUTF8Value("\n", END_OF_HEADER_ID); //Detecting this first but not right!! we did not close the revision??
    
	}
	
}
