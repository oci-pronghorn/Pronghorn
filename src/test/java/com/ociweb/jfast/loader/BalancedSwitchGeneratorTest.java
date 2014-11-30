package com.ociweb.jfast.loader;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.jfast.generator.BalancedSwitchGenerator;

public class BalancedSwitchGeneratorTest {

    @Test
    public void generateSmall() {
        
        int[] values = new int[] {1,23,3,24,65};
        String[] code   = new String[] {"a();","b();","c();","d();","e();"};
        
        StringBuilder target = new StringBuilder();
        
        BalancedSwitchGenerator bsg = new BalancedSwitchGenerator("x");
        bsg.generate("",target, values, code);
        
        String str = target.toString();
        assertTrue(str,str.contains("if ((x&0x40)==0) {"));
        //assertTrue(str,str.contains("assert(65==x) : \"found value of \"+x;"));
        
    }
 
    @Test
    public void generateLarge() {//worst case where there are no matching bits to split upon
        
        int[] values = new int[] {0,1,2,4,8,16,32,64,128,256,512,1024,2048};
        String[] code   = new String[values.length];
        int j = values.length;
        while (--j>=0) {
            code[j] = "x"+values[j]+"();";
        }
        
        StringBuilder target = new StringBuilder();
        
        BalancedSwitchGenerator bsg = new BalancedSwitchGenerator("x");
        bsg.generate("",target, values, code);
        
        String str = target.toString();
        //System.err.println(str);
        assertTrue(str,str.contains("if ((x&0x800)==0) {"));
        
    }
    
    
    
    
}
