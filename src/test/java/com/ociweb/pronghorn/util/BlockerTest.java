package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class BlockerTest {

    
    @Test
    public void simpleUsageTest() {
        
        Blocker block = new Blocker(12);
        
        block.until(34, 201);
        block.until(17, 21);
        block.until(42, 101);
        
        assertTrue(block.isBlocked(42));
        assertTrue(block.isBlocked(34));
        assertTrue(block.isBlocked(17));
        
        assertEquals(17, block.nextReleased(40, -1));
        assertEquals(-1, block.nextReleased(41, -1));
       
        assertTrue(block.isBlocked(42));
        assertTrue(block.isBlocked(34));
        assertFalse(block.isBlocked(17));
        
        //we get 42 before 34 because it expires first.
        assertEquals(42, block.nextReleased(340, -1));
        assertEquals(34, block.nextReleased(340, -1));
        assertEquals(-1, block.nextReleased(341, -1));
        

    }
    
}
