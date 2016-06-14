package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class BlockerTest {

    
    @Test
    public void simpleUsageTest() {
        
        Blocker block = new Blocker(12);
        
        block.until(42, 101);
        block.until(34, 201);
        block.until(17, 21);
        
        assertEquals(Blocker.BlockStatus.Blocked, block.status(42, 99));
        assertEquals(Blocker.BlockStatus.Released, block.status(42, 101));
        assertEquals(Blocker.BlockStatus.None, block.status(42, 101));
        
        assertEquals(Blocker.BlockStatus.Blocked, block.status(34, 199));
        assertEquals(Blocker.BlockStatus.Released, block.status(34, 201));
        assertEquals(Blocker.BlockStatus.None, block.status(34, 222));
        
        
    }
    
}
