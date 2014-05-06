//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import java.nio.ByteBuffer;

import com.ociweb.jfast.error.FASTException;

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
    private static final int BLOCK_SIZE = 512;// 4096;//128;// in bytes

    private static final int BLOCK_SIZE_LAZY = (BLOCK_SIZE * 3) + (BLOCK_SIZE >> 1);
    private static final int POS_POS_SHIFT = 28;
    private static final int POS_POS_MASK = 0xFFFFFFF; // top 4 are bit pos,
                                                       // bottom 28 are byte pos

    private final FASTOutput output;
    final byte[] buffer;

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
    private int limit;

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

        // TODO: Z, writing to one end and reading the other of flushSkips may
        // be causing performance issues
        this.flushSkipsSize = maxGroupCount * 2;
        this.flushSkips = new int[flushSkipsSize];// this may grow very large,
                                                  // to fields per group

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
                System.arraycopy(writer.buffer, sourceOffset, writer.buffer, targetOffset, flushRequest);
            }
            writer.nextBlockSize = BLOCK_SIZE - (reqLength - flushRequest);
            writer.pendingPosition = sourceOffset + flushRequest;
        }
    }

    private static void finishBlockAndLeaveRemaining(int sourceOffset, int targetOffset, int reqLength, PrimitiveWriter writer) {
        // more to flush than we need
        if (sourceOffset != targetOffset) {
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
    public final void writeByteArrayData(byte[] data, int offset, int length) {
        if (limit > buffer.length - length) {
            output.flush();
        }
        System.arraycopy(data, offset, buffer, limit, length);
        limit += length;
    }

    // data position is modified
    public final void writeByteArrayData(ByteBuffer data) {
        final int len = data.remaining();
        if (limit > buffer.length - len) {
            output.flush();
        }

        data.get(buffer, limit, len);
        limit += len;
    }

    // position is NOT modified
    public final void writeByteArrayData(ByteBuffer data, int pos, int lenToSend) {

        if (limit > buffer.length - lenToSend) {
            output.flush();
        }

        int stop = pos + lenToSend;
        while (pos < stop) {
            buffer[limit++] = data.get(pos++);
        }

    }

    public final void writeNull() {
        if (limit >= buffer.length) {
            output.flush();
        }
        buffer[limit++] = (byte) 0x80;
    }

    public final void writeLongSignedOptional(long value) {

        if (value >= 0) {
            writeLongSignedPos(value + 1, this);
        } else {
            writeLongSignedNeg(value, this);
        }

    }

    public final void writeLongSigned(long value) {

        if (value >= 0) {
            writeLongSignedPos(value, this);
        } else {
            writeLongSignedNeg(value, this);
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

    public final void writeLongUnsigned(long value) {
        if (value < 0x0000000000000080l) {
            if (buffer.length - limit < 1) {
                output.flush();
            }
        } else {
            if (value < 0x0000000000004000l) {
                if (buffer.length - limit < 2) {
                    output.flush();
                }
            } else {

                if (value < 0x0000000000200000l) {
                    if (buffer.length - limit < 3) {
                        output.flush();
                    }
                } else {

                    if (value < 0x0000000010000000l) {
                        if (buffer.length - limit < 4) {
                            output.flush();
                        }
                    } else {
                        if (value < 0x0000000800000000l) {

                            if (buffer.length - limit < 5) {
                                output.flush();
                            }
                        } else {
                            writeLongUnsignedSlow(value, this);
                            return;
                        }
                        buffer[limit++] = (byte) (((value >> 28) & 0x7F));
                    }
                    buffer[limit++] = (byte) (((value >> 21) & 0x7F));
                }
                buffer[limit++] = (byte) (((value >> 14) & 0x7F));
            }
            buffer[limit++] = (byte) (((value >> 7) & 0x7F));
        }
        buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
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

    public final void writeIntegerSignedOptional(int value) {
        if (value >= 0) {
            writeIntegerSignedPos(value + 1, this);
        } else {
            writeIntegerSignedNeg(value, this);
        }
    }

    public final void writeIntegerSigned(int value) {
        if (value >= 0) {
            writeIntegerSignedPos(value, this);
        } else {
            writeIntegerSignedNeg(value, this);
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

    public final void writeIntegerUnsigned(int value) {
        if (value < 0) {
            if (buffer.length - limit < 5) {
                output.flush();
            }
            buffer[limit++] = (byte) (((value >> 28) & 0x7F));
            buffer[limit++] = (byte) (((value >> 21) & 0x7F));
            buffer[limit++] = (byte) (((value >> 14) & 0x7F));
            buffer[limit++] = (byte) (((value >> 7) & 0x7F));
            buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
            return;
        }
        // assert(value>=0) :
        // "Java limitation must code this case special to reconstruct unsigned on the wire";
        if (value < 0x00000080) {
            if (buffer.length - limit < 1) {
                output.flush();
            }
        } else {
            if (value < 0x00004000) {
                if (buffer.length - limit < 2) {
                    output.flush();
                }
            } else {
                if (value < 0x00200000) {
                    if (buffer.length - limit < 3) {
                        output.flush();
                    }
                } else {
                    if (value < 0x10000000) {
                        if (buffer.length - limit < 4) {
                            output.flush();
                        }
                    } else {
                        if (buffer.length - limit < 5) {
                            output.flush();
                        }
                        buffer[limit++] = (byte) (((value >> 28) & 0x7F));
                    }
                    buffer[limit++] = (byte) (((value >> 21) & 0x7F));
                }
                buffer[limit++] = (byte) (((value >> 14) & 0x7F));
            }
            buffer[limit++] = (byte) (((value >> 7) & 0x7F));
        }
        buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
    }

    // /////////////////////////////////
    // New PMAP writer implementation
    // /////////////////////////////////

    // called only at the beginning of a group.
    public final void openPMap(int maxBytes) {

        // if (tempHead>=0) {
        // System.arraycopy(temp, 0, buffer, tempIdx, tempHead+1);
        // tempIdx = -1;
        // tempHead = -1;
        // }

        assert (maxBytes > 0) : "Do not call openPMap if it is not expected to be used.";

        if (limit > buffer.length - maxBytes) {
            output.flush();
        }

        // save the current partial byte.
        // always save because pop will always load
        if (safetyStackDepth > 0) {
            int s = safetyStackDepth - 1;
            assert (s >= 0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";

            // NOTE: can inc pos pos because it will not overflow.
            long stackFrame = safetyStackPosPos[s];
            int idx = (int) stackFrame & POS_POS_MASK;
            if (0 != (buffer[idx] = (byte) pMapByteAccum)) {
                // set the last known non zero bit so we can avoid scanning for
                // it.
                flushSkips[(int) (stackFrame >> 32)] = idx;
            }
            safetyStackPosPos[s] = (((long) pMapIdxWorking) << POS_POS_SHIFT) | idx;

        }
        // NOTE: pos pos, new position storage so top bits are always unset and
        // no need to set.

        safetyStackPosPos[safetyStackDepth++] = (((long) flushSkipsIdxLimit) << 32) | limit;
        flushSkips[flushSkipsIdxLimit++] = limit + 1;// default minimum size for
                                                     // present PMap
        flushSkips[flushSkipsIdxLimit++] = (limit += maxBytes);// this will
                                                               // remain as the
                                                               // fixed limit

        // reset so we can start accumulating bits in the new pmap.
        pMapIdxWorking = 7;
        pMapByteAccum = 0;

    }

    // called only at the end of a group.
    public final void closePMap() {

        // if (tempHead>=0) {
        // System.arraycopy(temp, 0, buffer, tempIdx, tempHead+1);
        // tempIdx = -1;
        // tempHead = -1;
        // }

        // ///
        // the PMap is ready for writing.
        // bit writes will go to previous bitmap location
        // ///
        // push open writes
        int s = --safetyStackDepth;
        assert (s >= 0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";

        // final byte to be saved into the feed. //NOTE: pos pos can inc because
        // it does not roll over.

        if (0 != (buffer[(int) (POS_POS_MASK & safetyStackPosPos[s]++)] = (byte) pMapByteAccum)) {
            // close is too late to discover overflow so it is NOT done here.
            //
            long stackFrame = safetyStackPosPos[s];
            // set the last known non zero bit so we can avoid scanning for it.
            buffer[(flushSkips[(int) (stackFrame >> 32)] = (int) (stackFrame & POS_POS_MASK)) - 1] |= 0x80;
        } else {
            // must set stop bit now that we know where pmap stops.
            buffer[flushSkips[(int) (safetyStackPosPos[s] >> 32)] - 1] |= 0x80;
        }

        // restore the old working bits if there is a previous pmap.
        if (safetyStackDepth > 0) {

            // NOTE: pos pos will not roll under so we can just subtract
            long posPos = safetyStackPosPos[safetyStackDepth - 1];
            pMapByteAccum = buffer[(int) (posPos & POS_POS_MASK)];
            pMapIdxWorking = (byte) (0xF & (posPos >> POS_POS_SHIFT));

        }

        // ensure low-latency for groups, or
        // if we can reset the safety stack and we have one block ready go ahead
        // and flush
        if (minimizeLatency != 0 || (0 == safetyStackDepth && (limit - position) > (BLOCK_SIZE_LAZY))) { // one
                                                                                                         // block
                                                                                                         // and
                                                                                                         // a
                                                                                                         // bit
                                                                                                         // left
                                                                                                         // over
                                                                                                         // so
                                                                                                         // we
                                                                                                         // need
                                                                                                         // bigger.
            output.flush();
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

    public final void writeTextASCII(CharSequence value) {

        int length = value.length();
        if (0 == length) {
            encodeZeroLengthASCII();
            return;
        } else if (limit > buffer.length - length) {
            assert (length < buffer.length) : "Internal buffer is only: " + buffer.length
                    + "B but this text requires a length of: " + length + "B";
            // if it was not zero and was too long then flush
            output.flush();
        }
        int c = 0;
        while (--length > 0) {
            buffer[limit++] = (byte) value.charAt(c++);
        }
        buffer[limit++] = (byte) (0x80 | value.charAt(c));

    }

    public final void writeTextASCIIAfter(int start, CharSequence value) {

        int length = value.length() - start;
        if (0 == length) {
            encodeZeroLengthASCII();
            return;
        } else if (limit > buffer.length - length) {
            // if it was not zero and was too long flush
            output.flush();
        }
        int c = start;
        while (--length > 0) {
            buffer[limit++] = (byte) value.charAt(c++);
        }
        buffer[limit++] = (byte) (0x80 | value.charAt(c));

    }

    public final void writeTextASCIIBefore(CharSequence value, int stop) {

        int length = stop;
        if (0 == length) {
            encodeZeroLengthASCII();
            return;
        } else if (limit > buffer.length - length) {
            // if it was not zero and was too long flush
            output.flush();
        }
        int c = 0;
        while (--length > 0) {
            buffer[limit++] = (byte) value.charAt(c++);
        }
        buffer[limit++] = (byte) (0x80 | value.charAt(c));

    }

    private void encodeZeroLengthASCII() {
        if (limit > buffer.length - 2) {
            output.flush();
        }
        buffer[limit++] = (byte) 0;
        buffer[limit++] = (byte) 0x80;
    }

    public void writeTextASCII(char[] value, int offset, int length) {

        if (0 == length) {
            encodeZeroLengthASCII();
            return;
        } else if (limit > buffer.length - length) {
            // if it was not zero and was too long flush
            output.flush();
        }
        while (--length > 0) {
            buffer[limit++] = (byte) value[offset++];
        }
        buffer[limit++] = (byte) (0x80 | value[offset]);
    }

    public void writeTextUTF(CharSequence value) {
        int len = value.length();
        int c = 0;
        while (c < len) {
            encodeSingleChar(value.charAt(c++));
        }
    }

    public void writeTextUTFBefore(CharSequence value, int stop) {
        int c = 0;
        while (c < stop) {
            encodeSingleChar(value.charAt(c++));
        }
    }

    public void writeTextUTFAfter(int start, CharSequence value) {
        int len = value.length();
        int c = start;
        while (c < len) {
            encodeSingleChar(value.charAt(c++));
        }
    }

    public void writeTextUTF(char[] value, int offset, int length) {
        while (--length >= 0) {
            encodeSingleChar(value[offset++]);
        }
    }

    private void encodeSingleChar(int c) {

        if (c <= 0x007F) {
            // code point 7
            if (limit > buffer.length - 1) {
                output.flush();
            }
            buffer[limit++] = (byte) c;
        } else {
            if (c <= 0x07FF) {
                // code point 11
                if (limit > buffer.length - 2) {
                    output.flush();
                }
                buffer[limit++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
            } else {
                if (c <= 0xFFFF) {
                    // code point 16
                    if (limit > buffer.length - 3) {
                        output.flush();
                    }
                    buffer[limit++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                } else {
                    encodeSingleCharSlow(c);
                }
                buffer[limit++] = (byte) (0x80 | ((c >> 6) & 0x3F));
            }
            buffer[limit++] = (byte) (0x80 | ((c) & 0x3F));
        }
    }

    protected void encodeSingleCharSlow(int c) {
        if (c < 0x1FFFFF) {
            // code point 21
            if (limit > buffer.length - 4) {
                output.flush();
            }
            buffer[limit++] = (byte) (0xF0 | ((c >> 18) & 0x07));
        } else {
            if (c < 0x3FFFFFF) {
                // code point 26
                if (limit > buffer.length - 5) {
                    output.flush();
                }
                buffer[limit++] = (byte) (0xF8 | ((c >> 24) & 0x03));
            } else {
                if (c < 0x7FFFFFFF) {
                    // code point 31
                    if (limit > buffer.length - 6) {
                        output.flush();
                    }
                    buffer[limit++] = (byte) (0xFC | ((c >> 30) & 0x01));
                } else {
                    throw new UnsupportedOperationException("can not encode char with value: " + c);
                }
                buffer[limit++] = (byte) (0x80 | ((c >> 24) & 0x3F));
            }
            buffer[limit++] = (byte) (0x80 | ((c >> 18) & 0x3F));
        }
        buffer[limit++] = (byte) (0x80 | ((c >> 12) & 0x3F));
    }

    // //////////////////////////
    // //////////////////////////

    // TODO: A, Copy, Increment and Delta writes need to have source field as well as target. for Int/Long Signed/Unsigned Normal/Optional
    public void writeIntegerUnsignedCopy(int value, int target, int source, int[] dictionary) {

        if (value == dictionary[source]) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerUnsigned(dictionary[target] = value);
        }
    }

    public void writeIntegerUnsignedCopyOptional(int value, int target, int source, int[] dictionary) {

        if (++value == dictionary[source]) {// not null and matches
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerUnsigned(dictionary[target] = value);
        }
    }

    public void writeIntegerUnsignedDefault(int value, int constDefault) {

        if (value == constDefault) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerUnsigned(value);
        }
    }

    public void writeIntegerUnsignedDefaultOptional(int value, int constDefault) {

        if (++value == constDefault) {// not null and matches
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerUnsigned(value);
        }
    }

    public void writeIntegerUnsignedIncrement(int value, int target, int source, int[] dictionary) {

        int incVal;
        if (value == (incVal = dictionary[source] + 1)) {
            dictionary[target] = incVal;
            writePMapBit((byte) 0, this);
        } else {
            dictionary[target] = value;
            writePMapBit((byte) 1, this);
            writeIntegerUnsigned(value);
        }
    }

    public void writeIntegerUnsignedIncrementOptional(int value, int target, int source, int[] dictionary) {

        if (0 != dictionary[source] && value == (dictionary[target] = dictionary[source] + 1)) {
            writePMapBit((byte) 0, this);
        } else {
            int tmp = dictionary[target] = 1 + value;
            writePMapBit((byte) 1, this);
            writeIntegerUnsigned(tmp);
        }
    }

    public void writeIntegerUnsignedDelta(int value, int target, int source, int[] dictionary) {
        writeLongSigned(value - (long) dictionary[source]);
        dictionary[target] = value;
    }

    public void writeIntegerUnsignedDeltaOptional(int value, int target, int source, int[] dictionary) {
        long delta = value - (long) dictionary[source];
        writeLongSigned(delta >= 0 ? 1 + delta : delta);
        dictionary[target] = value;
    }

    public void writeIntegerSignedCopy(int value, int target, int source, int[] dictionary) {
        if (value == dictionary[source]) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerSigned(dictionary[target] = value);
        }
    }

    public void writeIntegerSignedCopyOptional(int value, int target, int source, int[] dictionary) {
        if (value == dictionary[source]) {// not null and matches
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerSigned(dictionary[target] = value);
        }
    }

    public void writeIntegerSignedDefault(int value, int constDefault) {

        if (value == constDefault) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerSigned(value);
        }
    }

    public void writeIntegerSignedDefaultOptional(int value, int constDefault) {
        if (value == constDefault) {// matches
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerSigned(value);
        }
    }

    public void writeIntegerSignedIncrement(int value, int target, int source, int[] dictionary) {

        dictionary[target] = value;
        if (value == (dictionary[source] + 1)) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerSigned(value);
        }
    }

    public void writeIntegerSignedIncrementOptional(int value, int last) {
        if (0 != last && value == 1 + last) {// not null and matches
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeIntegerSigned(value);
        }
    }

    public void writeIntegerSignedDelta(int value, int target, int source, int[] dictionary) {
        int last = dictionary[source];
        if (value > 0 == last > 0) {
            writeIntegerSigned(value - last);
            dictionary[target] = value;
        } else {
            writeLongSigned(value - (long) last);
            dictionary[target] = value;
        }
    }

    public void writeIntegerSignedDeltaOptional(int value, int target, int source, int[] dictionary) {
        int last = dictionary[source];
        if (value > 0 == last > 0) {
            int dif = value - last;
            dictionary[target] = value;
            writeIntegerSigned(dif >= 0 ? 1 + dif : dif);
        } else {
            long dif = value - (long) last;
            dictionary[target] = value;
            writeLongSigned(dif >= 0 ? 1 + dif : dif);
        }
    }

    public void writeLongUnsignedCopy(long value, int target, int source, long[] dictionary) {

        if (value == dictionary[source]) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeLongUnsigned(dictionary[target] = value);
        }
    }

    public void writeLongUnsignedCopyOptional(long value, int target, int source, long[] dictionary) {
        if (value == dictionary[source]) {// not null and matches
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeLongUnsigned(dictionary[target] = value);
        }
    }

    public void writeLongUnsignedDefault(long value, long constDefault) {
        if (value == constDefault) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeLongUnsigned(value);
        }
    }

    public void writneLongUnsignedDefaultOptional(long value, long constDefault) {
        // room for zero
        if (++value == constDefault) {// not null and matches
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeLongUnsigned(value);
        }
    }

    public void writeLongUnsignedIncrement(long value, int target, int source, long[] dictionary) {
        long incVal = dictionary[source] + 1;
        if (value == incVal) {
            dictionary[target] = incVal;
            writePMapBit((byte) 0, this);
        } else {
            dictionary[target] = value;
            writePMapBit((byte) 1, this);
            writeLongUnsigned(value);
        }
    }

    public void writeLongUnsignedIncrementOptional(long value, int target, int source, long[] dictionary) {
        if (0 != dictionary[source] && value == (dictionary[target] = dictionary[source] + 1)) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeLongUnsigned(dictionary[target] = 1 + value);
        }
    }

    public void writeLongSignedCopy(long value, int target, int source, long[] dictionary) {
        if (value == dictionary[source]) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeLongSigned(dictionary[target] = value);
        }
    }

    public void writeLongSignedDefault(long value, long constDefault) {
        if (value == constDefault) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeLongSigned(value);
        }
    }

    public void writeLongSignedIncrement(long value, long last) {
        if (value == (1 + last)) {
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeLongSigned(value);
        }
    }

    public void writeLongSignedIncrementOptional(long value, long last) {
        if (0 != last && value == (1 + last)) {// not null and matches
            writePMapBit((byte) 0, this);
        } else {
            writePMapBit((byte) 1, this);
            writeLongSigned(value);
        }
    }

}
