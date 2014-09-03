//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import java.nio.ByteBuffer;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.stream.FASTRingBufferReader;

/**
 * PrimitiveWriter
 * 
 * Must be final and not implement any interface or be abstract. In-lining the
 * primitive methods of this class provides much of the performance needed by
 * this library.
 * 
 * 
 * @author Nathan Tippy
 * 
 */

public final class PrimitiveWriter {

    private static final int POS_POS_SHIFT = 28;
    private static final int POS_POS_MASK = 0xFFFFFFF; // top 4 are bit pos,
                                                       // bottom 28 are byte pos

    public final FASTOutput output;
    public final byte[] buffer;
    public final int bufferSize;

    private final int minimizeLatency;
    private final long[] safetyStackPosPos;// low 28, location where the last
                                           // byte was written to the pmap as
                                           // bits are written
                                           // mid 04, working bit position 1-7
                                           // for next/last bit to be written.
                                           // top 32, location (in skip list)
                                           // where location of stopBytes+1 and
                                           // end of max pmap length is found.

    private int safetyStackDepth; // maximum depth of the stacks above
    private int position;
    public int limit;

    // both bytes but class def likes int much better for alignment
    public int pMapIdxWorking = 7;
    public int pMapByteAccum = 0;

    private long totalWritten;
    private final int[] flushSkips;// list of all skip nodes produced at the end
                                   // of pmaps, may grow large with poor
                                   // templates.
    private final int flushSkipsSize;
    private int flushSkipsIdxLimit; // where we add the new one, end of the list
    private int flushSkipsIdxPos;// next limit to use. as skips are consumed
                                 // this pointer moves forward.

    private int nextBlockSize = -1; // not bigger than BLOCK_SIZE
    private int nextBlockOffset = -1; // position to begin copy data from
    private int pendingPosition = 0; // new position after the read

    private final int mustFlush;
    
    public PrimitiveWriter(int initBufferSize, FASTOutput output, boolean minimizeLatency) {

        // TODO: X, POS_POS_MASK can be shortened to only match the length of
        // buffer. but then buffer always must be a power of two.

        this.bufferSize=initBufferSize;
        this.buffer = new byte[bufferSize];
        this.position = 0;
        this.limit = 0;
        this.minimizeLatency = minimizeLatency ? 1 : 0;
        
        //must have enough room for the maximum number of groups that
        //will appear within the buffer, because the data is all variable 
        //length and that defines the smallest group as 1 byte. This stack
        //must be the same depth as the buffer size.
        this.safetyStackPosPos = new long[bufferSize]; 

        this.output = output;
        
        //NOTE: for high latency large throughput this must be as big as every group that can fit into initBufferSize
        //TODO: B, optimize this later by getting the smallest group size from template config and passing it in 
        this.flushSkipsSize = initBufferSize;//HACK for now assuming average message is no more than 1 bytes
        this.mustFlush = initBufferSize/2;// - (largestMessageSize*2);
        
        // this may grow very large, to fields per group
        this.flushSkips = new int[flushSkipsSize];

        output.init(new DataTransfer(this));
    }

    public static void reset(PrimitiveWriter writer) {
        writer.position = 0;
        writer.limit = 0;
        writer.safetyStackDepth = 0;
        writer.pMapIdxWorking = 7;
        writer.pMapByteAccum = 0;
        writer.flushSkipsIdxLimit = 0;
        writer.flushSkipsIdxPos = 0;

        writer.totalWritten = 0;

    }

    public static long totalWritten(PrimitiveWriter writer) {
        return writer.totalWritten;
    }

    public static int nextBlockSize(PrimitiveWriter writer) {
        // return block size if the block is available
        if (writer.nextBlockSize > 0 || writer.position == writer.limit) {
           // assert (writer.nextBlockSize != 0 || writer.position == writer.limit) : "nextBlockSize must be zero of position is at limit";
            return writer.nextBlockSize;
        }
        // block was not available so build it
        // This check greatly helps None/Delta operations which do not use pmap.
        if (writer.flushSkipsIdxPos == writer.flushSkipsIdxLimit) {
            // all the data we have lines up with the end of the skip limit
            // nothing to skip so flush the full block
            int avail = computeFlushToIndex(writer) - (writer.nextBlockOffset = writer.position);
            writer.pendingPosition = writer.position + (writer.nextBlockSize = avail);
        } else {
            buildNextBlockWithSkips(writer);
        }
        return writer.nextBlockSize;
    }

    // TODO: X, investigate moving first block to the rest first.
    private static void buildNextBlockWithSkips(PrimitiveWriter writer) {
        int sourceOffset = writer.position;
        int targetOffset = writer.position;
        int reqLength = writer.bufferSize;

        // do not change this value after this point we are committed to this
        // location.
        writer.nextBlockOffset = targetOffset;

        int localFlushSkipsIdxPos = writer.flushSkipsIdxPos;

        int sourceStop = writer.flushSkips[localFlushSkipsIdxPos];

        // invariant for the loop
        int temp = writer.flushSkipsSize - 2;
        final int localLastValid = (writer.flushSkipsIdxLimit < temp) ? writer.flushSkipsIdxLimit : temp;
        final int endOfData = writer.limit;
        final int finalStop = targetOffset + reqLength;

        int flushRequest = sourceStop - sourceOffset;
        int targetStop = targetOffset + flushRequest;

        // this loop may have many more iterations than one might expect, ensure
        // it only uses final and local values.
        // flush in parts that avoid the skip pos
        while (localFlushSkipsIdxPos < localLastValid && // stop if there are no
                                                         // more skip blocks
                sourceStop < endOfData && // stop at the end of the data
                targetStop <= finalStop // stop at the end if the BLOCK
        ) {

            // keep accumulating
            if (targetOffset != sourceOffset) {  
                //TODO: X, direct write without this move would help lower latency but this design helps throughput.
                System.arraycopy(writer.buffer, sourceOffset, writer.buffer, targetOffset, flushRequest);
            }
            // stop becomes start so we can build a contiguous block
            targetOffset = targetStop;
            //
            sourceOffset = writer.flushSkips[++localFlushSkipsIdxPos]; // new position
                                                                // in second
                                                                // part of flush
                                                                // skips
            // sourceStop is beginning of new skip.
            targetStop = targetOffset
                    + (flushRequest = (sourceStop = writer.flushSkips[++localFlushSkipsIdxPos]) - sourceOffset);

        }

        reqLength = finalStop - targetOffset; // remaining bytes required
        writer.flushSkipsIdxPos = localFlushSkipsIdxPos; // write back the local
                                                  // changes

        finishBuildingBlock(endOfData, sourceOffset, targetOffset, reqLength, writer);
    }

    private static void finishBuildingBlock(final int endOfData, int sourceOffset, int targetOffset, int reqLength, PrimitiveWriter writer) {
        int flushRequest = endOfData - sourceOffset;
        if (flushRequest >= reqLength) {
            finishBlockAndLeaveRemaining(sourceOffset, targetOffset, reqLength, writer);
        } else {
            // not enough data to fill full block, we are out of data to push.

            // reset to zero to save space if possible
            if (writer.flushSkipsIdxPos == writer.flushSkipsIdxLimit) {
                writer.flushSkipsIdxPos = writer.flushSkipsIdxLimit = 0;
            }
            // keep accumulating
            if (sourceOffset != targetOffset) {
                //TODO: X, direct write without this move would help lower latency but this design helps throughput.
                System.arraycopy(writer.buffer, sourceOffset, writer.buffer, targetOffset, flushRequest);
            }
   //         System.err.println("error here:"+ reqLength+" "+flushRequest+"   "+writer.position+"   "+writer.limit);
            
            writer.nextBlockSize = writer.bufferSize - (reqLength - flushRequest);
            writer.pendingPosition = sourceOffset + flushRequest;
        }
    }

    private static void finishBlockAndLeaveRemaining(int sourceOffset, int targetOffset, int reqLength, PrimitiveWriter writer) {
        // more to flush than we need
        if (sourceOffset != targetOffset) {
            //TODO: X, direct write without this move would help lower latency but this design helps throughput.
            System.arraycopy(writer.buffer, sourceOffset, writer.buffer, targetOffset, reqLength);
        }
        writer.nextBlockSize = writer.bufferSize;
        writer.pendingPosition = sourceOffset + reqLength;
    }

    public static int nextOffset(PrimitiveWriter writer) {
        if (writer.nextBlockSize <= 0) {
            throw new FASTException();
        }
        int nextOffset = writer.nextBlockOffset;

        writer.totalWritten += writer.nextBlockSize;

        writer.position = writer.pendingPosition;
        writer.nextBlockSize = -1;
        writer.nextBlockOffset = -1;
        writer.pendingPosition = 0;

        if (0 == writer.safetyStackDepth && 0 == writer.flushSkipsIdxLimit && writer.position == writer.limit) {
            writer.position = writer.limit = 0;
        }
      //  System.err.println("xxx "+writer.position+" "+writer.limit);
        //if nextBlock size is not large enought to get to the end of the message eg 255 but need 257 we will have 3 left
        //TODO: A, must write remaining bytes if it takes us to limit and is smaller than block?
        
        return nextOffset;
    }

    public static final int bytesReadyToWrite(PrimitiveWriter writer) {
        return writer.limit - writer.position;
    }

    public static final void flush(PrimitiveWriter writer) { // flush all
        writer.output.flush();
    }

    protected static int computeFlushToIndex(PrimitiveWriter writer) {
        if (writer.safetyStackDepth > 0) {
            // only need to check first entry on stack the rest are larger
            // values
            // NOTE: using safetyStackPosPos here may not be the best performant idea.
            int safetyLimit = (((int) writer.safetyStackPosPos[0]) & POS_POS_MASK) - 1;
            return (safetyLimit < writer.limit ? safetyLimit : writer.limit);
        } else {
            return writer.limit;
        }
    }

    // this requires the null adjusted length to be written first.
    public static final void writeByteArrayData(byte[] data, int offset, int length, PrimitiveWriter writer) {
        if (writer.limit > writer.buffer.length - length) {
            writer.output.flush();
        }
        System.arraycopy(data, offset, writer.buffer, writer.limit, length);
        writer.limit += length;
    }
    
    public static final void writeByteArrayData(byte[] data, int offset, int length, int mask, PrimitiveWriter writer) {
        if (writer.limit > writer.buffer.length - length) {
            writer.output.flush();
        }
        
        int i = 0;
        while (i<length) {
            writer.buffer[writer.limit+i]=data[mask&(i+offset)];
            i++;
        }
        
        //System.arraycopy(data, offset, writer.buffer, writer.limit, length);
        writer.limit += length;
    }

    // data position is modified
    public static final void writeByteArrayData(ByteBuffer data, PrimitiveWriter writer) {
        final int len = data.remaining();
        if (writer.limit > writer.buffer.length - len) {
            writer.output.flush();
        }

        data.get(writer.buffer, writer.limit, len);
        writer.limit += len;
    }

    // position is NOT modified
    public static final void writeByteArrayData(ByteBuffer data, int pos, int lenToSend, PrimitiveWriter writer) {

        if (writer.limit > writer.buffer.length - lenToSend) {
            writer.output.flush();
        }

        int stop = pos + lenToSend;
        while (pos < stop) {
            writer.buffer[writer.limit++] = data.get(pos++);
        }

    }

    public static final void writeNull(PrimitiveWriter writer) {
        if (writer.limit >= writer.buffer.length) {
            writer.output.flush();
        }
        writer.buffer[writer.limit++] = (byte) 0x80;
    }

    public static final void writeLongSignedOptional(long value, PrimitiveWriter writer) {

        if (value >= 0) {
            writeLongSignedPos(value + 1, writer);
        } else {
            writeLongSignedNeg(value, writer);
        }

    }

    public static final void writeLongSigned(long value, PrimitiveWriter writer) {

        if (value >= 0) {
            writeLongSignedPos(value, writer);
        } else {
            writeLongSignedNeg(value, writer);
        }

    }

    private static final void writeLongSignedNeg(long value, PrimitiveWriter writer) {
        // using absolute value avoids tricky word length issues
        long absv = -value;

        if (absv <= 0x0000000000000040l) {
            if (absv < 0) { // Must be most neg long because it will remain
                            // negative.
                writeLongSignedMostNegative(writer);
                return;
            }

            if (writer.buffer.length - writer.limit < 1) {
                writer.output.flush();
            }
        } else {
            if (absv <= 0x0000000000002000l) {
                if (writer.buffer.length - writer.limit < 2) {
                    writer.output.flush();
                }
            } else {

                if (absv <= 0x0000000000100000l) {
                    if (writer.buffer.length - writer.limit < 3) {
                        writer.output.flush();
                    }
                } else {

                    if (absv <= 0x0000000008000000l) {
                        if (writer.buffer.length - writer.limit < 4) {
                            writer.output.flush();
                        }
                    } else {
                        if (absv <= 0x0000000400000000l) {

                            if (writer.buffer.length - writer.limit < 5) {
                                writer.output.flush();
                            }
                        } else {
                            writeLongSignedNegSlow(absv, value, writer);
                            return;
                        }
                        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
                    }
                    writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
                }
                writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
            }
            writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        }
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));

    }

    private static void writeLongSignedMostNegative(PrimitiveWriter writer) {
        if (writer.limit > writer.buffer.length - 10) {
            writer.output.flush();
        }
        // encode the most negative possible number
        writer.buffer[writer.limit++] = (byte) (0x7F); // 8... .... .... ....
        writer.buffer[writer.limit++] = (byte) (0x00); // 7F.. .... .... ....
        writer.buffer[writer.limit++] = (byte) (0x00); // . FE .... .... ....
        writer.buffer[writer.limit++] = (byte) (0x00); // ...1 FC.. .... ....
        writer.buffer[writer.limit++] = (byte) (0x00); // .... .3F8 .... ....
        writer.buffer[writer.limit++] = (byte) (0x00); // .... ...7 F... ....
        writer.buffer[writer.limit++] = (byte) (0x00); // .... .... .FE. ....
        writer.buffer[writer.limit++] = (byte) (0x00); // .... .... ...1 FC..
        writer.buffer[writer.limit++] = (byte) (0x00); // .... .... .... 3F8.
        writer.buffer[writer.limit++] = (byte) (0x80); // .... .... .... ..7f
    }

    private static final void writeLongSignedNegSlow(long absv, long value, PrimitiveWriter writer) {
        if (absv <= 0x0000020000000000l) {
            if (writer.buffer.length - writer.limit < 6) {
                writer.output.flush();
            }
        } else {
            writeLongSignedNegSlow2(absv, value, writer);
        }

        // used by all
        writer.buffer[writer.limit++] = (byte) (((value >> 35) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));
    }

    private static void writeLongSignedNegSlow2(long absv, long value, PrimitiveWriter write) {
        if (absv <= 0x0001000000000000l) {
            if (write.buffer.length - write.limit < 7) {
                write.output.flush();
            }
        } else {
            if (absv <= 0x0080000000000000l) {
                if (write.buffer.length - write.limit < 8) {
                    write.output.flush();
                }
            } else {
                if (write.buffer.length - write.limit < 9) {
                    write.output.flush();
                }
                write.buffer[write.limit++] = (byte) (((value >> 56) & 0x7F));
            }
            write.buffer[write.limit++] = (byte) (((value >> 49) & 0x7F));
        }
        write.buffer[write.limit++] = (byte) (((value >> 42) & 0x7F));
    }

    private static final void writeLongSignedPos(long value, PrimitiveWriter writer) {

        if (value < 0x0000000000000040l) {
            if (writer.buffer.length - writer.limit < 1) {
                writer.output.flush();
            }
        } else {
            if (value < 0x0000000000002000l) {
                if (writer.buffer.length - writer.limit < 2) {
                    writer.output.flush();
                }
            } else {

                if (value < 0x0000000000100000l) {
                    if (writer.buffer.length - writer.limit < 3) {
                        writer.output.flush();
                    }
                } else {

                    if (value < 0x0000000008000000l) {
                        if (writer.buffer.length - writer.limit < 4) {
                            writer.output.flush();
                        }
                    } else {
                        if (value < 0x0000000400000000l) {

                            if (writer.buffer.length - writer.limit < 5) {
                                writer.output.flush();
                            }
                        } else {
                            writeLongSignedPosSlow(value, writer);
                            return;
                        }
                        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
                    }
                    writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
                }
                writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
            }
            writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        }
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));
    }

    private static final void writeLongSignedPosSlow(long value, PrimitiveWriter writer) {
        if (value < 0x0000020000000000l) {
            if (writer.buffer.length - writer.limit < 6) {
                writer.output.flush();
            }
        } else {
            if (value < 0x0001000000000000l) {
                if (writer.buffer.length - writer.limit < 7) {
                    writer.output.flush();
                }
            } else {
                if (value < 0x0080000000000000l) {
                    if (writer.buffer.length - writer.limit < 8) {
                        writer.output.flush();
                    }
                } else {
                    if (value < 0x4000000000000000l) {
                        if (writer.buffer.length - writer.limit < 9) {
                            writer.output.flush();
                        }
                    } else {
                        if (writer.buffer.length - writer.limit < 10) {
                            writer.output.flush();
                        }
                        writer.buffer[writer.limit++] = (byte) (((value >> 63) & 0x7F));
                    }
                    writer.buffer[writer.limit++] = (byte) (((value >> 56) & 0x7F));
                }
                writer.buffer[writer.limit++] = (byte) (((value >> 49) & 0x7F));
            }
            writer.buffer[writer.limit++] = (byte) (((value >> 42) & 0x7F));
        }

        // used by all
        writer.buffer[writer.limit++] = (byte) (((value >> 35) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));
    }

    public static final void writeLongUnsigned(long value, PrimitiveWriter writer) {
        
        
        
        if (value < 0x0000000000000080l) {
            if (writer.buffer.length - writer.limit < 1) {
                writer.output.flush();
            }
        } else {
            if (value < 0x0000000000004000l) {
                if (writer.buffer.length - writer.limit < 2) {
                    writer.output.flush();
                }
            } else {

                if (value < 0x0000000000200000l) {
                    if (writer.buffer.length - writer.limit < 3) {
                        writer.output.flush();
                    }
                } else {

                    if (value < 0x0000000010000000l) {
                        if (writer.buffer.length - writer.limit < 4) {
                            writer.output.flush();
                        }
                    } else {
                        if (value < 0x0000000800000000l) {

                            if (writer.buffer.length - writer.limit < 5) {
                                writer.output.flush();
                            }
                        } else {
                            writeLongUnsignedSlow(value, writer);
                            return;
                        }
                        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
                    }
                    writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
                }
                writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
            }
            writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        }
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));
    }

    private static final void writeLongUnsignedSlow(long value, PrimitiveWriter writer) {
        if (value < 0x0000040000000000l) {
            if (writer.buffer.length - writer.limit < 6) {
                writer.output.flush();
            }
        } else {
            writeLongUnsignedSlow2(value, writer);
        }

        // used by all
        writer.buffer[writer.limit++] = (byte) (((value >> 35) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));

    }

    private static void writeLongUnsignedSlow2(long value, PrimitiveWriter writer) {
        if (value < 0x0002000000000000l) {
            if (writer.buffer.length - writer.limit < 7) {
                writer.output.flush();
            }
        } else {
            if (value < 0x0100000000000000l) {
                if (writer.buffer.length - writer.limit < 8) {
                    writer.output.flush();
                }
            } else {
                if (value < 0x8000000000000000l) {
                    if (writer.buffer.length - writer.limit < 9) {
                        writer.output.flush();
                    }
                } else {
                    if (writer.buffer.length - writer.limit < 10) {
                        writer.output.flush();
                    }
                    writer.buffer[writer.limit++] = (byte) (((value >> 63) & 0x7F));
                }
                writer.buffer[writer.limit++] = (byte) (((value >> 56) & 0x7F));
            }
            writer.buffer[writer.limit++] = (byte) (((value >> 49) & 0x7F));
        }
        writer.buffer[writer.limit++] = (byte) (((value >> 42) & 0x7F));
    }

    public static final void writeIntegerSignedOptional(int value, PrimitiveWriter writer) {
        if (value >= 0) {
            writeIntegerSignedPos(value + 1, writer);
        } else {
            writeIntegerSignedNeg(value, writer);
        }
    }

    public static final void writeIntegerSigned(int value, PrimitiveWriter writer) {
        if (value >= 0) {
            writeIntegerSignedPos(value, writer);
        } else {
            writeIntegerSignedNeg(value, writer);
        }
    }

    private static void writeIntegerSignedNeg(int value, PrimitiveWriter writer) {
        // using absolute value avoids tricky word length issues
        int absv = -value;

        if (absv <= 0x00000040) {
            if (absv < 0) {// Integer.MIN_VALUE is the same neg value after -
                           // operator
                writeIntegerSignedMostNegative(writer);
                return;
            }

            if (writer.buffer.length - writer.limit < 1) {
                writer.output.flush();
            }
        } else {
            if (absv <= 0x00002000) {
                if (writer.buffer.length - writer.limit < 2) {
                    writer.output.flush();
                }
            } else {
                if (absv <= 0x00100000) {
                    if (writer.buffer.length - writer.limit < 3) {
                        writer.output.flush();
                    }
                } else {
                    if (absv <= 0x08000000) {
                        if (writer.buffer.length - writer.limit < 4) {
                            writer.output.flush();
                        }
                    } else {
                        if (writer.buffer.length - writer.limit < 5) {
                            writer.output.flush();
                        }
                        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
                    }
                    writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
                }
                writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
            }
            writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        }
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));

    }

    private static void writeIntegerSignedMostNegative(PrimitiveWriter writer) {
        if (writer.limit > writer.buffer.length - 5) {
            writer.output.flush();
        }
        // encode the most negative possible number
        // top stop bit remains zero but the next top bit is 1
        writer.buffer[writer.limit++] = (byte) (0x40);
        writer.buffer[writer.limit++] = (byte) (0x00);
        writer.buffer[writer.limit++] = (byte) (0x00);
        writer.buffer[writer.limit++] = (byte) (0x00);
        writer.buffer[writer.limit++] = (byte) (0x80);
        return;
    }

    private static void writeIntegerSignedPos(int value, PrimitiveWriter writer) {

        if (value < 0x00000040) {
            if (writer.buffer.length - writer.limit < 1) {
                writer.output.flush();
            }
        } else {
            if (value < 0x00002000) {
                if (writer.buffer.length - writer.limit < 2) {
                    writer.output.flush();
                }
            } else {
                if (value < 0x00100000) {
                    if (writer.buffer.length - writer.limit < 3) {
                        writer.output.flush();
                    }
                } else {
                    if (value < 0x08000000) {
                        if (writer.buffer.length - writer.limit < 4) {
                            writer.output.flush();
                        }
                    } else {
                        if (writer.buffer.length - writer.limit < 5) {
                            writer.output.flush();
                        }
                        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
                    }
                    writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
                }
                writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
            }
            writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        }
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));
    }

    public static final void writeIntegerUnsigned(int value, PrimitiveWriter writer) {
        if (value < 0) {
            writeIntegerUnsignedRollover(value, writer);
            return;
        }
        // assert(value>=0) :
        // "Java limitation must code this case special to reconstruct unsigned on the wire";
        if (value < 0x00000080) {
            if (writer.buffer.length - writer.limit < 1) {
                writer.output.flush();
            }
        } else {
            if (value < 0x00004000) {
                if (writer.buffer.length - writer.limit < 2) {
                    writer.output.flush();
                }
            } else {
                if (value < 0x00200000) {
                    if (writer.buffer.length - writer.limit < 3) {
                        writer.output.flush();
                    }
                } else {
                    if (value < 0x10000000) {
                        if (writer.buffer.length - writer.limit < 4) {
                            writer.output.flush();
                        }
                    } else {
                        if (writer.buffer.length - writer.limit < 5) {
                            writer.output.flush();
                        }
                        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
                    }
                    writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
                }
                writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
            }
            writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        }
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));
    }

    private static void writeIntegerUnsignedRollover(int value, PrimitiveWriter writer) {
        if (writer.buffer.length - writer.limit < 5) {
            writer.output.flush();
        }
        writer.buffer[writer.limit++] = (byte) (((value >> 28) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 21) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 14) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value >> 7) & 0x7F));
        writer.buffer[writer.limit++] = (byte) (((value & 0x7F) | 0x80));
        return;
    }

    // /////////////////////////////////
    // New PMAP writer implementation
    // /////////////////////////////////

    // called only at the beginning of a group.
    public static final void openPMap(int maxBytes, PrimitiveWriter writer) {

               
      //  System.err.println("write maxBytes "+maxBytes+" at "+(writer.limit+writer.totalWritten));

        assert (maxBytes > 0) : "Do not call openPMap if it is not expected to be used.";

        if (writer.limit > writer.buffer.length - maxBytes) {
            writer.output.flush();
        }

        // save the current partial byte.
        // always save because pop will always load
        if (writer.safetyStackDepth > 0) {
            int s = writer.safetyStackDepth - 1;
            assert (s >= 0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";

            // NOTE: can inc pos pos because it will not overflow.
            long stackFrame = writer.safetyStackPosPos[s];
            int idx = (int) stackFrame & POS_POS_MASK;                                    
            if (0 != (writer.buffer[idx] = (byte) writer.pMapByteAccum)) {
                // set the last known non zero bit so we can avoid scanning for
                // it.
                writer.flushSkips[(int) (stackFrame >> 32)] = idx;
            }
            writer.safetyStackPosPos[s] = (((long) writer.pMapIdxWorking) << POS_POS_SHIFT) | idx;

   //         System.err.println("open pmap2 set safety depth "+writer.safetyStackDepth+" value "+writer.limit+" or "+idx);

        }
        // NOTE: pos pos, new position storage so top bits are always unset and
        // no need to set.

  //      System.err.println("open pmap set safety depth "+writer.safetyStackDepth+" value "+writer.limit);
        
        writer.safetyStackPosPos[writer.safetyStackDepth++] = (((long) writer.flushSkipsIdxLimit) << 32) | writer.limit;
        writer.flushSkips[writer.flushSkipsIdxLimit++] = writer.limit + 1;// default minimum size for
                                                     // present PMap
        writer.flushSkips[writer.flushSkipsIdxLimit++] = (writer.limit += maxBytes);// this will
                                                               // remain as the
                                                               // fixed limit

        // reset so we can start accumulating bits in the new pmap.
        writer.pMapIdxWorking = 7;
        writer.pMapByteAccum = 0;

    }

    // called only at the end of a group.
    public static final void closePMap(PrimitiveWriter writer) {
        
    //    System.err.println("close pmap "+writer.position+" "+writer.limit);
        
        
        // ///
        // the PMap is ready for writing.
        // bit writes will go to previous bitmap location
        // ///
        // push open writes
        int s = --writer.safetyStackDepth;
        assert (s >= 0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";

        // final byte to be saved into the feed. //NOTE: pos pos can inc because
        // it does not roll over.

        if (0 != (writer.buffer[(int) (POS_POS_MASK & writer.safetyStackPosPos[s]++)] = (byte) writer.pMapByteAccum)) {
            // close is too late to discover overflow so it is NOT done here.
            //
            long stackFrame = writer.safetyStackPosPos[s];
            // set the last known non zero bit so we can avoid scanning for it.
            writer.buffer[(writer.flushSkips[(int) (stackFrame >> 32)] = (int) (stackFrame & POS_POS_MASK)) - 1] |= 0x80;
        } else {
            // must set stop bit now that we know where pmap stops.
            writer.buffer[writer.flushSkips[(int) (writer.safetyStackPosPos[s] >> 32)] - 1] |= 0x80;
        }

        // restore the old working bits if there is a previous pmap.
        if (writer.safetyStackDepth > 0) {

            // NOTE: pos pos will not roll under so we can just subtract
            long posPos = writer.safetyStackPosPos[writer.safetyStackDepth - 1];
            writer.pMapByteAccum = writer.buffer[(int) (posPos & POS_POS_MASK)];
            writer.pMapIdxWorking = (byte) (0xF & (posPos >> POS_POS_SHIFT));
        } 

        // ensure low-latency for groups, or
        // if reset the safety stack and we have one block ready go ahead and flush
        
        //NOTE: to only flush at the end of full messages use  0 == writer.safetyStackDepth 
        //      however for low latency we flush after every fragment and 
        //      for high throughput we wait until the buffer is filled to mustFlush
        
        if (writer.minimizeLatency != 0 || writer.limit > writer.mustFlush ) { 
            writer.output.flush();
           // System.err.println("flush");
        }// else {
         //   System.err.println("no flush "+writer.limit+" "+writer.mustFlush);
      //  }
        

    }

    // called by ever field that needs to set a bit either 1 or 0
    // must be fast because it is frequently called.
    public static final void writePMapBit(byte bit, PrimitiveWriter writer) {
        if (0 == --writer.pMapIdxWorking) { //TODO: B, can remove this conditional the same way it was done in the reader.
            writeNextPMapByte(bit, writer);
            writer.pMapIdxWorking = 7; //not needed when this gets in-lined so it is done here
        } else {
            writer.pMapByteAccum |= (bit << writer.pMapIdxWorking);
        }
    }

    public static void writeNextPMapByte(byte bit, PrimitiveWriter writer) {

        // bits but it must be less! what if we cached the buffer writes?
      //     assert (writer.safetyStackDepth > 0) : "PMap must be open before write of bits.";
        int idx = (int) (POS_POS_MASK & writer.safetyStackPosPos[writer.safetyStackDepth - 1]++);

        // save this byte and if it was not a zero save that fact as well
        // //NOTE: pos pos will not rollover so can inc
        if (0 != (writer.buffer[idx] = (byte) (writer.pMapByteAccum | bit))) {   //TODO: C, code gen can remove this conditional when bit==1
            // set the last known non zero bit so we can avoid scanning for it.
            writer.flushSkips[(int) ( writer.safetyStackPosPos[writer.safetyStackDepth - 1] >> 32)] =  idx+1;
        }

        writer.pMapByteAccum = 0;
    }

    public static final void writeTextASCIIAfter(int start, byte[] value, int offset, int len, int mask, PrimitiveWriter writer) {

        int length = len - start;
        if (0 == length) {
            encodeZeroLengthASCII(writer);
            return;
        } else if (writer.limit > writer.buffer.length - length) {
            // if it was not zero and was too long flush
            writer.output.flush();
        }
        int c = start+offset;
        while (--length > 0) {
            writer.buffer[writer.limit++] = (byte) value[mask&c++];
        }
        writer.buffer[writer.limit++] = (byte) (0x80 | value[mask&c]);

    }
    
   
    public static final void writeTextASCIIBefore(byte[] value, int valueOffset, int valueMask, int stop, PrimitiveWriter writer) {

        int length = stop;
        if (0 == length) {
            encodeZeroLengthASCII(writer);
            return;
        } else if (writer.limit > writer.buffer.length - length) {
            // if it was not zero and was too long flush
            writer.output.flush();
        }
        int c = valueOffset;
        while (--length > 0) {
            writer.buffer[writer.limit++] = (byte) value[valueMask&c++];
        }
        writer.buffer[writer.limit++] = (byte) (0x80 | value[valueMask&c]);

    }

    private static void encodeZeroLengthASCII(PrimitiveWriter writer) {
        if (writer.limit > writer.buffer.length - 2) {
            writer.output.flush();
        }
        writer.buffer[writer.limit++] = (byte) 0;
        writer.buffer[writer.limit++] = (byte) 0x80;
    }

    public static void writeTextASCII(byte[] value, int offset, int length, PrimitiveWriter writer) {

        if (0 == length) {
            encodeZeroLengthASCII(writer);
            return;
        } else if (writer.limit > writer.buffer.length - length) {
            // if it was not zero and was too long flush
            writer.output.flush();
        }
        int len = length-1;
        System.arraycopy(value, offset, writer.buffer, writer.limit, len);
        writer.limit+=len;
        offset+=len;
        
        writer.buffer[writer.limit++] = (byte) (0x80 | value[offset]);
    }
    
    public static void writeTextASCII(byte[] value, int offset, int length, int mask,  PrimitiveWriter writer) {

        if (0 == length) {
            encodeZeroLengthASCII(writer);
            return;
        } else if (writer.limit > writer.buffer.length - length) {
            // if it was not zero and was too long flush
            writer.output.flush();
        }
        
        while (--length > 0) {
            writer.buffer[writer.limit++] = (byte) value[mask & offset++];
        }
        
        writer.buffer[writer.limit++] = (byte) (0x80 | value[mask & offset]);
    }

    public static void ensureSpace(int bytes, PrimitiveWriter writer) {
        
        if (writer.limit > writer.buffer.length - bytes) {
            writer.output.flush();
        }
        
    }    

   

}
