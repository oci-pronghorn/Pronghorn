package com.ociweb.pronghorn.util;

/**
 * Brancheless methods have more consistent execution time than their equivalent branching implementations.
 * The performance can be expected to be better than the worst case (branch prediction miss) but worse than the best case (branch prediction hit)
 * 
 * 
 * @author Nathan Tippy
 *
 */
public class Branchless {
   
    /**
     * return x==y?a:b; 
     */
    public static int ifEquals(int x, int y, int a, int b) {        
      //return x==y?a:b; 
      return onEqu1(x, y, a, b, ((x-y)-1)>>31);
    }
    
    /**
     * return x==0?a:b;
     */
    public static int ifZero(int x, int a, int b) {
        //return x==0?a:b; 
        return onZero1(x, a, b, (x-1)>>31);
    }

    private static int onZero1(int x, int a, int b, int tmp) {
        return onEqu2(a, b, ((x>>31)^tmp)&tmp);
    }

    private static int onEqu1(int x, int y, int a, int b, int tmp) {
        return onEqu2(a, b, (((x-y)>>31)^tmp)&tmp);
    }

    private static int onEqu2(int a, int b, int mask) {
        return (a&mask)|(b&(~mask));
    }

        
    
    
}
