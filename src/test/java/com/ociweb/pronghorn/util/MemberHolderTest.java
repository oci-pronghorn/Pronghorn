package com.ociweb.pronghorn.util;

import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;


public class MemberHolderTest {

    public final int iterations = 10000000;
    
    @Test
    public void testAddMembers() {
        
       
        MemberHolder holder = new MemberHolder(10);
        
        final int listId = 3;
        final int seed = 42;

        Random r = new Random(seed);
        
        populate(holder, listId, r);
        
        final Random expectedR = new Random(seed);
        
        holder.visit(listId, new MemberHolderVisitor() {
            int i = 0;
            @Override
            public void visit(long value) { 

                if (++i<100000) {
                    assertEquals(expectedR.nextInt(i), value);
                } else {
                    long lng = expectedR.nextLong() & 0x7FFFFFFFFFFFFFFFl;

                    if (lng!=value) {
                        System.out.println("expected:"+Long.toBinaryString(lng));
                        System.out.println("   found:"+Long.toBinaryString(value));
                    }
                    
                    assertEquals(lng, value);
                    
                }
            }
            @Override
            public void finished() {
                
            }
            
        });
        
        
    }
    
    @Test
    public void testContainsMembers() {
        
       
        MemberHolder holder = new MemberHolder(10);
        
        final int listId = 3;
        final int seed = 42;

        Random r = new Random(seed);

        populate(holder, listId, r);

        final Random expectedR = new Random(seed);
        
        int c = 0;
        if (++c<100000) {
            assertTrue(holder.containsCount(listId, expectedR.nextInt(c))>0);
        } else {
            long lng = expectedR.nextLong() & 0x7FFFFFFFFFFFFFFFl;
            
            assertTrue(holder.containsCount(listId, lng)>0);
            
        }
        
    }
    
    @Test
    public void testRemoveMembers() {
               
        MemberHolder holder = new MemberHolder(10);
        
        final int listId = 3;
        final int seed = 42;

        Random r = new Random(seed);

        populate(holder, listId, r);

        final Random removedR = new Random(seed);
        
         
        int j = 0;
        if (++j<100000) {
            int value = removedR.nextInt(j);            
            if (0==(value&1)) {
                while (holder.containsCount(listId, value)>0) {
                    holder.removeMember(listId, value);
                }
            }
        } else {
            long value = removedR.nextLong() & 0x7FFFFFFFFFFFFFFFl;
            
            if (0==(value&1)) { 
                while (holder.containsCount(listId, value)>0) {
                    holder.removeMember(listId, value);
                }
            }
            
        }
    
        final Random expectedR = new Random(seed);
        int c = 0;
        if (++c<100000) {
            int value = expectedR.nextInt(c);
            if (0==(value&1)) {
                assertTrue(holder.containsCount(listId, value)==0);
            } else {
                assertTrue(holder.containsCount(listId, value)>0);
            }
        } else {
            long value = expectedR.nextLong() & 0x7FFFFFFFFFFFFFFFl;
            if (0==(value&1)) {
                assertTrue(holder.containsCount(listId, value)==0);
            } else {
                assertTrue(holder.containsCount(listId, value)>0);
            }
            
        }
        
    } 
    
    
    private void populate(MemberHolder holder, final int listId, Random r) {
        int i = 0;
        int limit =iterations;
        while (++i<=limit) {        
            if (i<100000) { //force testing of small values
                holder.addMember(listId, r.nextInt(i));
            } else {
                long lng = r.nextLong() & 0x7FFFFFFFFFFFFFFFl;
   
                holder.addMember(listId, lng); //TODO: we have a bug where this does not work for very large negative longs, FIX.
            }
        }
    }
    
}
