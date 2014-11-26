package com.ociweb.jfast.util;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.ociweb.pronghorn.ring.util.hash.MurmurHash;

public class MurmurHashTest {

	
	@Test
	public void collideTest() {
		
		
		
		
		long j =        0xFFFFFFFF+1;
		final long k =  j-0xFFFFF;
		int seed = 104729;
		
		Map<Integer, byte[]> seen = new HashMap<Integer, byte[]>();
		Set<Integer> collides= new HashSet<Integer>();
		
		while (--j>k) {
			
			
			
			byte[] bytes = testString(j).getBytes(); //with very lont sequences this is less likely to collide
			
			Integer value = MurmurHash.hash32(bytes, 0, bytes.length, seed);
		
			if (seen.containsKey(value)) {
				collides.add(value);
				//primes still have collisions but each prime has different collisions 
				//System.err.println("found collision "+value+" for both "+new String(bytes)+" and "+new String(seen.get(value)));
			}
			
			seen.put(value, bytes);
						
		}
		//System.err.println("first round of collisions:"+collides.size());
		
		//System.err.println("teting second round using primes");
		seed = 17;//104729;//104393;
		j = 0xFFFFFFFF+1;
		
		int totalCollisions = 0;
		
		while (--j>k) {
			
			byte[] bytes = testString(j).getBytes();
			
			Integer value = MurmurHash.hash32(bytes, 0, bytes.length, seed);
		
			if (seen.containsKey(value) && collides.contains(value)) {
				totalCollisions++;
				System.err.println("found collision "+value+" for both "+new String(bytes)+" and "+new String(seen.get(value))+"  total "+totalCollisions);
				
			}
			
			seen.put(value, bytes);
						
		}
		
		//we are trying to show that using different seeds produce different collisions on different values
		assertTrue(totalCollisions < collides.size());
		
	}

	private String testString(long j) {
		
		return Long.toHexString(j)+Long.toHexString(j*13)+Long.toHexString(j*7)+Long.toHexString(j);
	}
	
	
}
