package com.ociweb.pronghorn.pipe;

public class PipeRegulator {
    
    private long regulatorTimeBase = System.currentTimeMillis();
    private long regulatorPositionBase = 0;
    private long divisor;
    
    /**
     * Helper method so the scheduler can get the number of MS that this stage should wait before reschedule.
     * 
     * This supports both regulating the consumer/producer of data based on contract requirements of the connected software.
     * 
     * @param maxMsgPerMs
     */
    public PipeRegulator(int maxMsgPerMs, int avgMsgSize) {
        this.divisor = (long)avgMsgSize*(long)maxMsgPerMs;
    }    
    
    public static <S extends MessageSchema> long computeRateLimitDelay(Pipe<S> pipe, long position, PipeRegulator regulator) {
        long expectedNow = regulator.regulatorTimeBase + ((position-regulator.regulatorPositionBase)/regulator.divisor);
        long dif = expectedNow-System.currentTimeMillis();
        
      //  System.out.println(dif+"  "+regulator.divisor);
        
        return dif;
    }
    
}
