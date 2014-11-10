package com.ociweb.jfast.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class MurmurHashTest {

	
	@Test
	public void collideTest() {
		
		int j = 0xFFFFFF+1;
		int k =  0xFFFFF;
		int seed = 7777;
		
		Map<Integer, byte[]> seen = new HashMap<Integer, byte[]>();
		Set<Integer> collides= new HashSet<Integer>();
		
		while (--j>k) {
			
			byte[] bytes = Integer.toHexString(j).getBytes();
			
			Integer value = MurmurHash.hash32(bytes, 0, bytes.length, seed);
		
			if (seen.containsKey(value)) {
				collides.add(value);
				//System.err.println("found collision "+value+" for both "+new String(bytes)+" and "+new String(seen.get(value)));
			}
			
			seen.put(value, bytes);
						
		}
		
		seed = seed+1;
		j = 0xFFFFFF+1;
		
		while (--j>k) {
			
			byte[] bytes = Integer.toHexString(j).getBytes();
			
			Integer value = MurmurHash.hash32(bytes, 0, bytes.length, seed);
		
			if (seen.containsKey(value) && collides.contains(value)) {
				
			//	System.err.println("found collision "+value+" for both "+new String(bytes)+" and "+new String(seen.get(value)));
				
			}
			
			seen.put(value, bytes);
						
		}
		
		
	}
	
	
}
