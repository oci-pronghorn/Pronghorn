package com.ociweb.pronghorn.util.math;

import java.util.Arrays;
import static org.junit.Assert.*;

import org.junit.Test;

/*
 * @Author Nathan Tippy
 */
public class PMathTest {

    @Test
    public void testFactors() {
        
        int length = 5;
        
        byte[] target = new byte[length];
        
        PMath.factors(120, target, 0, length, Integer.MAX_VALUE);
        assertTrue(Arrays.toString(target),Arrays.equals(new byte[]{3,1,1,0,0}, target));
        assertEquals(120,PMath.factorsToInt(target, 0, length, Integer.MAX_VALUE));
                
        PMath.factors(80, target, 0, length, Integer.MAX_VALUE);
        assertTrue(Arrays.toString(target),Arrays.equals(new byte[]{4,0,1,0,0}, target));      
        assertEquals(80,PMath.factorsToInt(target, 0, length, Integer.MAX_VALUE));
        
        length = 33;        
        target = new byte[33];
        
        PMath.factors(131*2, target, 0, length, Integer.MAX_VALUE);
        assertTrue(Arrays.toString(target),Arrays.equals(new byte[]{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0}, target));      
        assertEquals(131*2,PMath.factorsToInt(target, 0, length, Integer.MAX_VALUE));
                
    }
    

    @Test
    public void testFactorsWrapped() {
        
        int length = 4;
        
        byte[] target = new byte[length];
        
        PMath.factors(120, target, 2, length, 0x3);
        assertTrue(Arrays.toString(target),Arrays.equals(new byte[]{1,0,3,1}, target));
        assertEquals(120,PMath.factorsToInt(target, 2, length, 0x3));
                
        PMath.factors(80, target, 2, length, 0x3);
        assertTrue(Arrays.toString(target),Arrays.equals(new byte[]{1,0,4,0}, target));      
        assertEquals(80,PMath.factorsToInt(target, 2, length, 0x3));
        
        length = 32;        
        target = new byte[32];
        
        PMath.factors(131*2, target, 4, length, 0x1F);
        assertTrue(Arrays.toString(target),Arrays.equals(new byte[]{0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, target));      
        assertEquals(131*2,PMath.factorsToInt(target, 4, length, 0x1F));
                
    }
    
    @Test
    public void testGCM() {
        
        int length = 5;
        
        byte[] targetA = new byte[length];
        byte[] targetB = new byte[length];
        byte[] targetC = new byte[length];
                
        PMath.factors(120, targetA, 0, length, Integer.MAX_VALUE);
        PMath.factors(80, targetB, 0, length, Integer.MAX_VALUE);
        PMath.greatestCommonFactor(targetA, 0, length, Integer.MAX_VALUE, 
                                   targetB, 0, length, Integer.MAX_VALUE, 
                                   targetC, 0, length, Integer.MAX_VALUE);
        
        assertEquals(40,PMath.factorsToInt(targetC, 0, length, Integer.MAX_VALUE));
       
        
    }
    
    
    @Test
    public void testGCMWrapped() {
        
        int length = 4;
        
        byte[] targetA = new byte[length];
        byte[] targetB = new byte[length];
        byte[] targetC = new byte[length];
                
        PMath.factors(120, targetA, 2, length, 0x3);
        PMath.factors(80, targetB, 2, length, 0x3);
        PMath.greatestCommonFactor(targetA, 2, length, 0x3, 
                                   targetB, 2, length, 0x3, 
                                   targetC, 2, length, 0x3);
        
        assertEquals(40,PMath.factorsToInt(targetC, 2, length, 0x3));
       
        
    }
    
    @Test
    public void testMultiply() {
        
        int length = 5;
        
        byte[] targetA = new byte[length];
        byte[] targetB = new byte[length];
        byte[] targetC = new byte[length];
                
        PMath.factors(120, targetA, 0, length, Integer.MAX_VALUE);
        PMath.factors(80, targetB, 0, length, Integer.MAX_VALUE);
                
        PMath.addFactors(targetA, 0, length, Integer.MAX_VALUE, 
                        targetB, 0, length, Integer.MAX_VALUE, 
                        targetC, 0, length, Integer.MAX_VALUE);
        
        assertEquals(120*80,PMath.factorsToInt(targetC, 0, length, Integer.MAX_VALUE));
               
    }
    
    
    @Test
    public void testMultiplyWrapped() {
        
        int length = 4;
        
        byte[] targetA = new byte[length];
        byte[] targetB = new byte[length];
        byte[] targetC = new byte[length];
                
        PMath.factors(120, targetA, 2, length, 0x3);
        PMath.factors(80, targetB, 2, length, 0x3);
                
        PMath.addFactors(targetA, 2, length, 0x3, 
                        targetB, 2, length, 0x3, 
                        targetC, 2, length, 0x3);
        
        assertEquals(120*80,PMath.factorsToInt(targetC, 2, length, 0x3));
               
    }
        
    @Test
    public void testDivide() {
        
        int length = 5;
        
        byte[] targetA = new byte[length];
        byte[] targetB = new byte[length];
        byte[] targetC = new byte[length];
                
        PMath.factors(120, targetA, 0, length, Integer.MAX_VALUE);
        PMath.factors(12, targetB, 0, length, Integer.MAX_VALUE);
                
        PMath.removeFactors(targetA, 0, length, Integer.MAX_VALUE, 
                           targetB, 0, length, Integer.MAX_VALUE, 
                           targetC, 0, length, Integer.MAX_VALUE);
        
        assertEquals(120/12,PMath.factorsToInt(targetC, 0, length, Integer.MAX_VALUE));
               
    }
    
    @Test
    public void testDivideWrapped() {
        
        int length = 4;
        
        byte[] targetA = new byte[length];
        byte[] targetB = new byte[length];
        byte[] targetC = new byte[length];
                
        PMath.factors(120, targetA, 2, length, 0x3);
        PMath.factors(12, targetB, 2, length, 0x3);
                
        PMath.removeFactors(targetA, 2, length, 0x3, 
                           targetB, 2, length, 0x3, 
                           targetC, 2, length, 0x3);
        
        assertEquals(120/12,PMath.factorsToInt(targetC, 2, length, 0x3));
               
    }
    
    
    @Test
    public void testScheduler() {
      
        long[] schedulePeriods = {300, 70, 50 ,20}; //NOTE: sort these so they are longest to shortest (by time taken not frequency) do short read first.
                
        ScriptedSchedule schedule = PMath.buildScriptedSchedule(schedulePeriods);   
        
        assertEquals(394, schedule.script.length);
        assertEquals(10, schedule.commonClock);
        assertEquals(4, schedule.maxRun);     
        
    }
    
    
}
