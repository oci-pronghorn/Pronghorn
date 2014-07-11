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

    // TODO: X, the write to output is not implemented right it must send one
    // giant block when possible
    // TODO: X, we should have min and max block size? this may cover all cases.
    private static final int BLOCK_SIZE = 256;//256;//512;// 4096;//128;// in bytes

    private static final int BLOCK_SIZE_LAZY = (BLOCK_SIZE * 3) + (BLOCK_SIZE >> 1);
    private static final int POS_POS_SHIFT = 28;
    private static final int POS_POS_MASK = 0xFFFFFFF; // top 4 are bit pos,
                                                       // bottom 28 are byte pos

    public final FASTOutput output;
    public final byte[] buffer;

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
    private int pMapIdxWorking = 7;
    private int pMapByteAccum = 0;

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

    private boolean minimizeFlush = false;
    
    public PrimitiveWriter(int initBufferSize, FASTOutput output, int maxGroupCount, boolean minimizeLatency) {

        // NOTE: POS_POS_MASK can be shortened to only match the length of
        // buffer. but then buffer always must be a power of two.

        if (initBufferSize < BLOCK_SIZE_LAZY * 2) {
            initBufferSize = BLOCK_SIZE_LAZY * 2;
        }

        this.buffer = new byte[initBufferSize];
        this.position = 0;
        this.limit = 0;
        this.minimizeLatency = minimizeLatency ? 1 : 0;
        this.safetyStackPosPos = new long[maxGroupCount];

        this.output = output;
        this.flushSkipsSize = maxGroupCount * 2;
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
            assert (writer.nextBlockSize != 0 || writer.position == writer.limit) : "nextBlockSize must be zero of position is at limit";
            return writer.nextBlockSize;
        }
        // block was not available so build it
        // This check greatly helps None/Delta operations which do not use pmap.
        if (writer.flushSkipsIdxPos == writer.flushSkipsIdxLimit) {
            // all the data we have lines up with the end of the skip limit
            // nothing to skip so flush the full block
            int avail = computeFlushToIndex(writer) - (writer.nextBlockOffset = writer.position);
            writer.pendingPosition = writer.position + (writer.nextBlockSize = (avail >= BLOCK_SIZE) ? BLOCK_SIZE : avail);
        } else {
            buildNextBlockWithSkips(writer);
        }
        return writer.nextBlockSize;
    }

    // TODO: X, investigate moving first block to the rest first.
    private static void buildNextBlockWithSkips(PrimitiveWriter writer) {
        int sourceOffset = writer.position;
        int targetOffset = writer.position;
        int reqLength = BLOCK_SIZE;

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
            writer.nextBlockSize = BLOCK_SIZE - (reqLength - flushRequest);
            writer.pendingPosition = sourceOffset + flushRequest;
        }
    }

    private static void finishBlockAndLeaveRemaining(int sourceOffset, int targetOffset, int reqLength, PrimitiveWriter writer) {
        // more to flush than we need
        if (sourceOffset != targetOffset) {
            //TODO: X, direct write without this move would help lower latency but this design helps throughput.
            System.arraycopy(writer.buffer, sourceOffset, writer.buffer, targetOffset, reqLength);
        }
        writer.nextBlockSize = BLOCK_SIZE;
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

        return nextOffset;
    }

    public static final int bytesReadyToWrite(PrimitiveWriter writer) {
        return writer.limit - writer.position;
    }

    public static final void flush(PrimitiveWriter writer) { // flush all
        writer.output.flush();
    }

    protected static int computeFlushToIndex(PrimitiveWriter writer) {
        if (writer.safetyStackDepth > 0) {// TODO: T, this never happens according to
                                   // coverage test?
            // only need to check first entry on stack the rest are larger
            // values
            // NOTE: using safetyStackPosPos here may not be the best performant
            // idea.
            int safetyLimit = (((int) writer.safetyStackPosPos[0]) & POS_POS_MASK) - 1;// still
                                                                                // modifying
                                                                                // this
                                                                                // position
                                                                                // but
                                                                                // previous
                                                                                // is
                                                                                // ready
                                                                                // to
                                                                                // go.
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

    // /////////////////////////////////
    // New PMAP writer implementation
    // /////////////////////////////////

    // called only at the beginning of a group.
    public static final void openPMap(int maxBytes, PrimitiveWriter writer) {

        // if (tempHead>=0) {
        // System.arraycopy(temp, 0, buffer, tempIdx, tempHead+1);
        // tempIdx = -1;
        // tempHead = -1;
        // }

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

        }
        // NOTE: pos pos, new position storage so top bits are always unset and
        // no need to set.

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
        // if we can reset the safety stack and we have one block ready go ahead
        // and flush
        if (!writer.minimizeFlush) { //TODO: this maximizes bandwith usage and helps find bugs earlier
            if (writer.minimizeLatency != 0 ||
                (0 == writer.safetyStackDepth && (writer.limit - writer.position) > (BLOCK_SIZE_LAZY))) { 
                writer.output.flush();
            }
        } 

    }

    // called by ever field that needs to set a bit either 1 or 0
    // must be fast because it is frequently called.
    public static final void writePMapBit(byte bit, PrimitiveWriter writer) {
        if (0 == --writer.pMapIdxWorking) {
            // TODO: X, note we only corrupt the buffer cache line once every 7
            // bits but it must be less! what if we cached the buffer writes?
            assert (writer.safetyStackDepth > 0) : "PMap must be open before write of bits.";
            int idx = (int) (POS_POS_MASK & writer.safetyStackPosPos[writer.safetyStackDepth - 1]++);

            // save this byte and if it was not a zero save that fact as well
            // //NOTE: pos pos will not rollover so can inc
            if (0 != (writer.buffer[idx] = (byte) (bit == 0 ? writer.pMapByteAccum : (writer.pMapByteAccum | bit)))) {
                long stackFrame = writer.safetyStackPosPos[writer.safetyStackDepth - 1];
                // set the last known non zero bit so we can avoid scanning for
                // it.
                int lastPopulatedIdx = (int) (POS_POS_MASK & stackFrame);// one
                                                                         // has
                                                                         // been
                                                                         // added
                                                                         // for
                                                                         // exclusive
                                                                         // use
                                                                         // of
                                                                         // range
                // writing the pmap bit is the ideal place to detect overflow of
                // the bits based on expectations.
                assert (lastPopulatedIdx < writer.flushSkips[(int) (stackFrame >> 32) + 1]) : "Too many bits in PMAP.";
                writer.flushSkips[(int) (stackFrame >> 32)] = lastPopulatedIdx;
            }

            writer.pMapIdxWorking = 7;
            writer.pMapByteAccum = 0;

        } else {
            if (bit != 0) {
                writer.pMapByteAccum |= (bit << writer.pMapIdxWorking);
            }
        }
    }

    public static final void writeTextASCII(CharSequence value, PrimitiveWriter writer) {

        int length = value.length();
        if (0 == length) {
            encodeZeroLengthASCII(writer);
            return;
        } else if (writer.limit > writer.buffer.length - length) {
            assert (length < writer.buffer.length) : "Internal buffer is only: " + writer.buffer.length
                    + "B but this text requires a length of: " + length + "B";
            // if it was not zero and was too long then flush
            writer.output.flush();
        }
        int c = 0;
        while (--length > 0) {
            writer.buffer[writer.limit++] = (byte) value.charAt(c++);
        }
        writer.buffer[writer.limit++] = (byte) (0x80 | value.charAt(c));

    }

    public static final void writeTextASCIIAfter(int start, CharSequence value, PrimitiveWriter writer) {

        int length = value.length() - start;
        if (0 == length) {
            encodeZeroLengthASCII(writer);
            return;
        } else if (writer.limit > writer.buffer.length - length) {
            // if it was not zero and was too long flush
            writer.output.flush();
        }
        int c = start;
        while (--length > 0) {
            writer.buffer[writer.limit++] = (byte) value.charAt(c++);
        }
        writer.buffer[writer.limit++] = (byte) (0x80 | value.charAt(c));

    }

    public static final void writeTextASCIIBefore(CharSequence value, int stop, PrimitiveWriter writer) {

        int length = stop;
        if (0 == length) {
            encodeZeroLengthASCII(writer);
            return;
        } else if (writer.limit > writer.buffer.length - length) {
            // if it was not zero and was too long flush
            writer.output.flush();
        }
        int c = 0;
        while (--length > 0) {
            writer.buffer[writer.limit++] = (byte) value.charAt(c++);
        }
        writer.buffer[writer.limit++] = (byte) (0x80 | value.charAt(c));

    }

    private static void encodeZeroLengthASCII(PrimitiveWriter writer) {
        if (writer.limit > writer.buffer.length - 2) {
            writer.output.flush();
        }
        writer.buffer[writer.limit++] = (byte) 0;
        writer.buffer[writer.limit++] = (byte) 0x80;
    }

    public static void writeTextASCII(char[] value, int offset, int length, PrimitiveWriter writer) {

        if (0 == length) {
            encodeZeroLengthASCII(writer);
            return;
        } else if (writer.limit > writer.buffer.length - length) {
            // if it was not zero and was too long flush
            writer.output.flush();
        }
        while (--length > 0) {
            writer.buffer[writer.limit++] = (byte) value[offset++];
        }
        writer.buffer[writer.limit++] = (byte) (0x80 | value[offset]);
    }

    public static void ensureSpace(int bytes, PrimitiveWriter writer) {
        
        if (writer.limit > writer.buffer.length - bytes) {
            writer.output.flush();
        }
        
    }    

    

    // //////////////////////////
    // //////////////////////////

    public static final void writeIntegerUnsignedCopy(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {

        if (value == dictionary[source]) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerUnsigned(dictionary[target] = value, writer);
        }
    }

    public static final void writeIntegerUnsignedCopyOptional(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {

        if (++value == dictionary[source]) {// not null and matches
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerUnsigned(dictionary[target] = value, writer);
        }
    }

    public static final void writeIntegerUnsignedDefault(int value, int constDefault, PrimitiveWriter writer) {

        if (value == constDefault) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerUnsigned(value, writer);
        }
    }

    public static final void writeIntegerUnsignedDefaultOptional(int value, int constDefault, PrimitiveWriter writer) {

        if (++value == constDefault) {// not null and matches
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerUnsigned(value, writer);
        }
    }

    public static final void writeIntegerUnsignedIncrement(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {

        int incVal;
        if (value == (incVal = dictionary[source] + 1)) {
            dictionary[target] = incVal;
            writePMapBit((byte) 0, writer);
        } else {
            dictionary[target] = value;
            writePMapBit((byte) 1, writer);
            writeIntegerUnsigned(value, writer);
        }
    }

    public static final void writeIntegerUnsignedIncrementOptional(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {

        if (0 != dictionary[source] && value == (dictionary[target] = dictionary[source] + 1)) {
            writePMapBit((byte) 0, writer);
        } else {
            int tmp = dictionary[target] = 1 + value;
            writePMapBit((byte) 1, writer);
            writeIntegerUnsigned(tmp, writer);
        }
    }

    public static void writeIntegerUnsignedDelta(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {
        writeLongSigned(value - (long) dictionary[source], writer);
        dictionary[target] = value;
    }

    public static void writeIntegerUnsignedDeltaOptional(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {
        long delta = value - (long) dictionary[source];
        writeLongSigned(delta >= 0 ? 1 + delta : delta, writer);
        dictionary[target] = value;
    }

    public static void writeIntegerSignedCopy(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {
        if (value == dictionary[source]) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerSigned(dictionary[target] = value, writer);
        }
    }

    public static void writeIntegerSignedCopyOptional(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {
        if (value == dictionary[source]) {// not null and matches
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerSigned(dictionary[target] = value, writer);
        }
    }

    public static void writeIntegerSignedDefault(int value, int constDefault, PrimitiveWriter writer) {

        if (value == constDefault) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerSigned(value, writer);
        }
    }

    public static void writeIntegerSignedDefaultOptional(int value, int constDefault, PrimitiveWriter writer) {
        if (value == constDefault) {// matches
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerSigned(value, writer);
        }
    }

    public static void writeIntegerSignedIncrement(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {

        dictionary[target] = value;
        if (value == (dictionary[source] + 1)) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerSigned(value, writer);
        }
    }

    public static void writeIntegerSignedIncrementOptional(int value, int last, PrimitiveWriter writer) {
        if (0 != last && value == 1 + last) {// not null and matches
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeIntegerSigned(value, writer);
        }
    }

    public static void writeIntegerSignedDelta(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {
        int last = dictionary[source];
        if (value > 0 == last > 0) {
            writeIntegerSigned(value - last, writer);
            dictionary[target] = value;
        } else {
            writeLongSigned(value - (long) last, writer);
            dictionary[target] = value;
        }
    }

    public static void writeIntegerSignedDeltaOptional(int value, int target, int source, int[] dictionary, PrimitiveWriter writer) {
        int last = dictionary[source];
        if (value > 0 == last > 0) {
            int dif = value - last;
            dictionary[target] = value;
            writeIntegerSigned(dif >= 0 ? 1 + dif : dif, writer);
        } else {
            long dif = value - (long) last;
            dictionary[target] = value;
            writeLongSigned(dif >= 0 ? 1 + dif : dif, writer);
        }
    }

    public static void writeLongUnsignedCopy(long value, int target, int source, long[] dictionary, PrimitiveWriter writer) {

        if (value == dictionary[source]) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeLongUnsigned(dictionary[target] = value, writer);
        }
    }

    public static void writeLongUnsignedCopyOptional(long value, int target, int source, long[] dictionary, PrimitiveWriter writer) {
        if (value == dictionary[source]) {// not null and matches
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeLongUnsigned(dictionary[target] = value, writer);
        }
    }

    public static void writeLongUnsignedDefault(long value, long constDefault, PrimitiveWriter writer) {
        if (value == constDefault) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeLongUnsigned(value, writer);
        }
    }

    public static void writneLongUnsignedDefaultOptional(long value, long constDefault, PrimitiveWriter writer) {
        // room for zero
        if (++value == constDefault) {// not null and matches
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeLongUnsigned(value, writer);
        }
    }

    public static void writeLongUnsignedIncrement(long value, int target, int source, long[] dictionary, PrimitiveWriter writer) {
        long incVal = dictionary[source] + 1;
        if (value == incVal) {
            dictionary[target] = incVal;
            writePMapBit((byte) 0, writer);
        } else {
            dictionary[target] = value;
            writePMapBit((byte) 1, writer);
            writeLongUnsigned(value, writer);
        }
    }

    public static void writeLongUnsignedIncrementOptional(long value, int target, int source, long[] dictionary, PrimitiveWriter writer) {
        if (0 != dictionary[source] && value == (dictionary[target] = dictionary[source] + 1)) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeLongUnsigned(dictionary[target] = 1 + value, writer);
        }
    }

    public static void writeLongSignedCopy(long value, int target, int source, long[] dictionary, PrimitiveWriter writer) {
        if (value == dictionary[source]) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeLongSigned(dictionary[target] = value, writer);
        }
    }

    public static void writeLongSignedDefault(long value, long constDefault, PrimitiveWriter writer) {
        if (value == constDefault) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeLongSigned(value, writer);
        }
    }

    public static void writeLongSignedIncrement(long value, long last, PrimitiveWriter writer) {
        if (value == (1 + last)) {
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeLongSigned(value, writer);
        }
    }

    public static void writeLongSignedIncrementOptional(long value, long last, PrimitiveWriter writer) {
        if (0 != last && value == (1 + last)) {// not null and matches
            writePMapBit((byte) 0, writer);
        } else {
            writePMapBit((byte) 1, writer);
            writeLongSigned(value, writer);
        }
    }

}
