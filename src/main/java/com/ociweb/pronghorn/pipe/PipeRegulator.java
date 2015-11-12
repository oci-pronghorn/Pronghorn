package com.ociweb.pronghorn.pipe;

public class PipeRegulator {
    
    private long regulatorTimeBase = System.currentTimeMillis();
    private long regulatorPositionBase = 0;
    
    private final int regulatorMsgPerMsBase2;
    
    /**
     * Helper method so the scheduler can get the number of MS that this stage should wait before reschedule.
     * 
     * This supports both regulating the consumer/producer of data based on contract requirements of the connected software.
     * 
     * @param maxMsgPerMs
     */
    public PipeRegulator(int maxMsgPerMs) {
        this.regulatorMsgPerMsBase2 = (int)Math.ceil(Math.log(maxMsgPerMs)/Math.log(2));        
    }    
    
    public static <S extends MessageSchema> long computeRateLimitDelay(Pipe<S> pipe, long position, long now, PipeRegulator regulator) {
        long expectedNow = regulator.regulatorTimeBase + ((position-regulator.regulatorPositionBase) >> regulator.regulatorMsgPerMsBase2);
        if (expectedNow>now) {
            return expectedNow-now;
        } else {
            //restart the clock to prevent "banking of events" when nothing is getting passed.
            regulator.regulatorTimeBase = now;
            regulator.regulatorPositionBase = position;
            return 0;
        }
    }
    
}
