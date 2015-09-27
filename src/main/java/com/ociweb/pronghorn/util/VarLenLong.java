package com.ociweb.pronghorn.util;

import java.nio.ByteBuffer;

public class VarLenLong {

    //////////////////////////////////////////////////////////////////////////
    //Write signed long using variable length encoding as defined in FAST spec
    //////////////////////////////////////////////////////////////////////////
    
    public static final void writeLongSigned(long value, ByteBuffer byteBuffer) {
        if (0!=(value & 0x8000000000000000l )) {
            throw new UnsupportedOperationException("Out of bounds: Encoding does not support the use of the highest bit in signed long value.");
        }        
        
        if (value >=0) {
            writeLongSignedPos(value, byteBuffer);
        } else {
            writeLongSignedNeg(value, byteBuffer);
        }
    }

    private static final void writeLongSignedNeg(long value, ByteBuffer byteBuffer) {
        // using absolute value avoids tricky word length issues
        long absv = -value;

        if (absv <= 0x0000000000000040l) {
            if (absv < 0) { // Must be most neg long because it will remain
                            // negative.
                writeLongSignedMostNegative(byteBuffer);
                return;
            }
        } else {
            if (absv <= 0x0000000000002000l) {
            } else {
                if (absv <= 0x0000000000100000l) {
                } else {
                    if (absv <= 0x0000000008000000l) {
                    } else {
                        if (absv <= 0x0000000400000000l) {
                        } else {
                            writeLongSignedNegSlow(absv, value, byteBuffer);
                            return;
                        }
                        byteBuffer.put( (byte) (((value >> 28) & 0x7F)) );
                    }
                    byteBuffer.put( (byte) (((value >> 21) & 0x7F)) );
                }
                byteBuffer.put( (byte) (((value >> 14) & 0x7F)) );
            }
            byteBuffer.put( (byte) (((value >> 7) & 0x7F)) );
        }
        byteBuffer.put( (byte) (((value & 0x7F) | 0x80)) );
    }

    private static void writeLongSignedMostNegative(ByteBuffer byteBuffer) {
        //TODO: make this a single put of a constant array.
        // encode the most negative possible number
        byteBuffer.put( (byte) (0x7F)); // 8... .... .... ....
        byteBuffer.put( (byte) (0x00)); // 7F.. .... .... ....
        byteBuffer.put( (byte) (0x00)); // . FE .... .... ....
        byteBuffer.put( (byte) (0x00)); // ...1 FC.. .... ....
        byteBuffer.put( (byte) (0x00)); // .... .3F8 .... ....
        byteBuffer.put( (byte) (0x00)); // .... ...7 F... ....
        byteBuffer.put( (byte) (0x00)); // .... .... .FE. ....
        byteBuffer.put( (byte) (0x00)); // .... .... ...1 FC..
        byteBuffer.put( (byte) (0x00)); // .... .... .... 3F8.
        byteBuffer.put( (byte) (0x80)); // .... .... .... ..7f

    }

    private static final void writeLongSignedNegSlow(long absv, long value, ByteBuffer byteBuffer) {
        if (absv <= 0x0000020000000000l) {
        } else {
            writeLongSignedNegSlow2(absv, value, byteBuffer);
        }

        // used by all
        byteBuffer.put( (byte) (((value >> 35) & 0x7F)) );
        byteBuffer.put( (byte) (((value >> 28) & 0x7F)) );
        byteBuffer.put( (byte) (((value >> 21) & 0x7F)) );
        byteBuffer.put( (byte) (((value >> 14) & 0x7F)) );
        byteBuffer.put( (byte) (((value >> 7) & 0x7F)) );
        byteBuffer.put( (byte) (((value & 0x7F) | 0x80)) );
    }

    private static void writeLongSignedNegSlow2(long absv, long value, ByteBuffer byteBuffer) {
        if (absv <= 0x0001000000000000l) {
        } else {
            if (absv <= 0x0080000000000000l) {
            } else {
                byteBuffer.put( (byte) (((value >> 56) & 0x7F)) );
            }
            byteBuffer.put( (byte) (((value >> 49) & 0x7F)) );
        }
        byteBuffer.put( (byte) (((value >> 42) & 0x7F)) );
    }

    private static final void writeLongSignedPos(long value, ByteBuffer byteBuffer) {

        if (value < 0x0000000000000040l) {
        } else {
            if (value < 0x0000000000002000l) {
            } else {
                if (value < 0x0000000000100000l) {
                } else {
                    if (value < 0x0000000008000000l) {
                    } else {
                        if (value < 0x0000000400000000l) {
                        } else {
                            writeLongSignedPosSlow(value, byteBuffer);
                            return;
                        }
                        byteBuffer.put( (byte) (((value >> 28) & 0x7F)) );
                    }
                    byteBuffer.put( (byte) (((value >> 21) & 0x7F)) );
                }
                byteBuffer.put( (byte) (((value >> 14) & 0x7F)) );
            }
            byteBuffer.put( (byte) (((value >> 7) & 0x7F)) );
        }
        byteBuffer.put( (byte) (((value & 0x7F) | 0x80)) );
    }

    private static final void writeLongSignedPosSlow(long value, ByteBuffer byteBuffer) {
        if (value < 0x0000020000000000l) {
        } else {
            if (value < 0x0001000000000000l) {
            } else {
                if (value < 0x0080000000000000l) {
                } else {
                    if (value < 0x4000000000000000l) {
                    } else {
                        byteBuffer.put(  (byte) (((value >> 63) & 0x7F)) );
                    }
                    byteBuffer.put(  (byte) (((value >> 56) & 0x7F)) );
                }
                byteBuffer.put(  (byte) (((value >> 49) & 0x7F)) );
            }
            byteBuffer.put(  (byte) (((value >> 42) & 0x7F)) );
        }

        // used by all
        byteBuffer.put( (byte) (((value >> 35) & 0x7F)) );
        byteBuffer.put( (byte) (((value >> 28) & 0x7F)) );
        byteBuffer.put( (byte) (((value >> 21) & 0x7F)) );
        byteBuffer.put( (byte) (((value >> 14) & 0x7F)) );
        byteBuffer.put( (byte) (((value >> 7) & 0x7F)) );
        byteBuffer.put( (byte) (((value & 0x7F) | 0x80)) );

    }
    
    
    //////////////////////////////////////////////////////////////////////////
    //Read signed long using variable length encoding as defined in FAST spec
    //////////////////////////////////////////////////////////////////////////
    
    /**
     * Parse a 64 bit signed value from the buffer
     * 
     * @param reader
     * @return
     */
    public static long readLongSigned(ByteBuffer byteBuffer) {   
            byte v = byteBuffer.get();
            long accumulator = (~((long)(((v>>6)&1)-1)))&0xFFFFFFFFFFFFFF80l; 
            return (v < 0) ? accumulator |(v & 0x7F) : readLongSignedTail((accumulator | v) << 7,byteBuffer);
    }
    
    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static long readLongSignedTail(long a, ByteBuffer byteBuffer) {
        byte v = byteBuffer.get();
        return (v<0) ? a | (v & 0x7Fl) : readLongSignedTail((a | v) << 7,byteBuffer);
    }
    
}
