package com.ociweb.pronghorn.pipe;

public class PipeRegulator {
    
    private long regulatorTimeBaseNS = System.currentTimeMillis()*MS_TO_NS;
    private long regulatorPositionBase = 0;
    private long divisorSizeNS;
    private static final long MS_TO_NS = 1_000_000L;
    
    /**
     * Helper method so the scheduler can get the number of nanos that this stage should wait before reschedule.
     * 
     * This supports both regulating the consumer/producer of data based on contract requirements of the connected software.
     * 
     * @param maxMsgPerMs
     */
    public PipeRegulator(int maxMsgPerMs, int avgMsgSize) {
        this.divisorSizeNS = MS_TO_NS*(long)avgMsgSize*(long)maxMsgPerMs;
    }    
    
    public static <S extends MessageSchema<S>> long computeRateLimitDelay(Pipe<S> pipe, long position, PipeRegulator regulator) {
        long expectedNow = regulator.regulatorTimeBaseNS + ((position-regulator.regulatorPositionBase)/regulator.divisorSizeNS);
        return expectedNow-(System.currentTimeMillis()*MS_TO_NS);
    }
    
}
