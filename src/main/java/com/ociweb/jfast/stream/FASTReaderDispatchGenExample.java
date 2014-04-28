package com.ociweb.jfast.stream;
import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.StaticGlue;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;
//TODO B, Generate header imports from FASTReaderDispach, and build public constructor from same arguments.

public class FASTReaderDispatchGenExample extends FASTReaderDispatch {

    public FASTReaderDispatchGenExample(PrimitiveReader reader, DictionaryFactory dcr, int nonTemplatePMapSize,
            int[][] dictionaryMembers, int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript,
            int maxNestedGroupDepth, int ringBits, int ringTextBits) {
        super(reader, dcr, nonTemplatePMapSize, dictionaryMembers, maxTextLen, maxVectorLen, charGap, bytesGap,
                fullScript, maxNestedGroupDepth, ringBits, ringTextBits);
    }


    private void grp1(TextHeap textHeap, long[] rLongDictionary, long[] rLongInit, int[] rIntDictionary, int[] rIntInit, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        assert (gatherReadData(reader, activeScriptCursor));
        m0001_001(textHeap);
        m0001_002(rLongDictionary,rLongInit);
        m0001_003(rLongDictionary,rLongInit);
        m0001_004(rIntDictionary,rIntInit);
        m0001_005(rIntDictionary,rIntInit);
        m0001_006(rIntDictionary,rIntInit);
        m0001_007(rIntDictionary,rIntInit);
        m0001_008(rIntDictionary,rIntInit);
        m0001_009(rIntDictionary,rIntInit);
        m0001_00a(rIntDictionary,rIntInit);
        m0001_00b(rIntDictionary,rIntInit);
        m0001_00c(rbB,rbMask,rbRingBuffer);
        m0001_00d(rbB,rbMask,rbRingBuffer);
        m0001_00e(rbB,rbMask,rbRingBuffer);
        m0001_00f(rbB,rbMask,reader,rIntDictionary,rbRingBuffer);
        m0001_010(rbB,rbMask,reader,rIntDictionary,rbRingBuffer);
        m0001_011(rbB,rbMask,reader,rIntDictionary,rbRingBuffer);
        if (m0001_012(rbB,rbMask,rbRingBuffer,rIntDictionary,reader)) {return;};
        m0001_013(reader);
        m0001_014(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_015(rbB,rbMask,reader,rbRingBuffer);
        m0001_016(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_017(rbB,rbMask,reader,rbRingBuffer);
        m0001_018(rbB,rbMask,rbRingBuffer);
        m0001_019(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_01a(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_01b(rbB,rbMask,reader,rbRingBuffer);
        m0001_01c(rLongDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_01d(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_01e(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_01f(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_020(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_021(rbB,rbMask,reader,rbRingBuffer);
        m0001_022(rLongDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_023(rbB,rbMask,reader,rbRingBuffer);
        m0001_024(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_025(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_026(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_027(rbB,rbMask,reader,rbRingBuffer);
        m0001_028(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_029(reader);
        m0001_02a();
        activeScriptCursor=31;
    }
    private void m0001_001(TextHeap textHeap) {
            //genReadDictionaryTextReset(idx, textHeap)
            textHeap.reset(0x4/*idx*/);
        };
    private void m0001_002(long[] rLongDictionary,long[] rLongInit) {
            //genReadDictionaryLongReset(idx, rLongDictionary, rLongInit)
            rLongDictionary[0x0/*idx*/] = rLongInit[0x0/*idx*/];
        };
    private void m0001_003(long[] rLongDictionary,long[] rLongInit) {
            //genReadDictionaryLongReset(idx, rLongDictionary, rLongInit)
            rLongDictionary[0x1/*idx*/] = rLongInit[0x1/*idx*/];
        };
    private void m0001_004(int[] rIntDictionary,int[] rIntInit) {
            //genReadDictionaryIntegerReset(idx, rIntDictionary, rIntInit)
            rIntDictionary[0x4/*idx*/] = rIntInit[0x4/*idx*/];
        };
    private void m0001_005(int[] rIntDictionary,int[] rIntInit) {
            //genReadDictionaryIntegerReset(idx, rIntDictionary, rIntInit)
            rIntDictionary[0x8/*idx*/] = rIntInit[0x8/*idx*/];
        };
    private void m0001_006(int[] rIntDictionary,int[] rIntInit) {
            //genReadDictionaryIntegerReset(idx, rIntDictionary, rIntInit)
            rIntDictionary[0x9/*idx*/] = rIntInit[0x9/*idx*/];
        };
    private void m0001_007(int[] rIntDictionary,int[] rIntInit) {
            //genReadDictionaryIntegerReset(idx, rIntDictionary, rIntInit)
            rIntDictionary[0xa/*idx*/] = rIntInit[0xa/*idx*/];
        };
    private void m0001_008(int[] rIntDictionary,int[] rIntInit) {
            //genReadDictionaryIntegerReset(idx, rIntDictionary, rIntInit)
            rIntDictionary[0xb/*idx*/] = rIntInit[0xb/*idx*/];
        };
    private void m0001_009(int[] rIntDictionary,int[] rIntInit) {
            //genReadDictionaryIntegerReset(idx, rIntDictionary, rIntInit)
            rIntDictionary[0xc/*idx*/] = rIntInit[0xc/*idx*/];
        };
    private void m0001_00a(int[] rIntDictionary,int[] rIntInit) {
            //genReadDictionaryIntegerReset(idx, rIntDictionary, rIntInit)
            rIntDictionary[0xd/*idx*/] = rIntInit[0xd/*idx*/];
        };
    private void m0001_00b(int[] rIntDictionary,int[] rIntInit) {
            //genReadDictionaryIntegerReset(idx, rIntDictionary, rIntInit)
            rIntDictionary[0xe/*idx*/] = rIntInit[0xe/*idx*/];
        };
    private void m0001_00c(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x80000002/*constIdx*/;
            rbB[rbMask & rbRingBuffer.addPos++] = 0x3/*constLen*/;
        };
    private void m0001_00d(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x80000005/*constIdx*/;
            rbB[rbMask & rbRingBuffer.addPos++] = 0x1/*constLen*/;
        };
    private void m0001_00e(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x80000006/*constIdx*/;
            rbB[rbMask & rbRingBuffer.addPos++] = 0xd/*constLen*/;
        };
    private void m0001_00f(int[] rbB,int rbMask,PrimitiveReader reader,int[] rIntDictionary,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsigned(target, rbB, rbMask, reader, rIntDictionary, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[0x0/*target*/] = reader.readIntegerUnsigned();
        };
    private void m0001_010(int[] rbB,int rbMask,PrimitiveReader reader,int[] rIntDictionary,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsigned(target, rbB, rbMask, reader, rIntDictionary, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[0x1/*target*/] = reader.readIntegerUnsigned();
        };
    private void m0001_011(int[] rbB,int rbMask,PrimitiveReader reader,int[] rIntDictionary,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsigned(target, rbB, rbMask, reader, rIntDictionary, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[0x2/*target*/] = reader.readIntegerUnsigned();
        };
    private boolean m0001_012(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer,int[] rIntDictionary,PrimitiveReader reader) {
            //genReadLength(target, jumpToTarget, rbB, rbMask, rbRingBuffer, rIntDictionary, reader)
            int length;
            int value = rIntDictionary[0x3/*target*/] = length = reader.readIntegerUnsigned();
            rbB[rbMask & rbRingBuffer.addPos++] = value;
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                activeScriptCursor = 0x1f/*jumpToTarget*/;
                return true;
            } else {
                // jumpSequence = 0;
                sequenceCountStack[++sequenceCountStackHead] = length;
                return false;
           }
        };
    private void m0001_013(PrimitiveReader reader) {
            //genReadGroupPMapOpen(nonTemplatePMapSize, reader)
            reader.openPMap(0x3/*nonTemplatePMapSize*/);
        };
    private void m0001_014(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedCopy(0x4/*target*/, 0x4/*source*/, rIntDictionary, reader);
        };
    private void m0001_015(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/, reader);
        };
    private void m0001_016(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIICopy(idx, rbB, rbMask, reader, textHeap, rbRingBuffer)
             int len;
            if (PrimitiveReader.popPMapBit(reader)!=0) {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    len=StaticGlue.readASCIIToHeapValue(val, tmp, 0x4/*idx*/, textHeap, reader);
                } else {
                    len=StaticGlue.readASCIIToHeapNone(0x4/*idx*/, val, textHeap, reader);
                }
            } else {
                len = textHeap.valueLength(0x4/*idx*/);
            }
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x4/*idx*/, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
        };
    private void m0001_017(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedOptional(constAbsent, rbB, rbMask, reader, rbRingBuffer)
            int xi1;
            rbB[rbMask & rbRingBuffer.addPos++] = 0 == (xi1 = reader.readIntegerUnsigned()) ? 0x7fffffff/*constAbsent*/ : xi1 - 1;
        };
    private void m0001_018(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedConstant(constDefault, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x9/*constDefault*/;
        };
    private void m0001_019(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedCopy(0x8/*target*/, 0x8/*source*/, rIntDictionary, reader);
        };
    private void m0001_01a(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedIncrement(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedIncrement(0x9/*target*/, 0x9/*source*/, rIntDictionary);
        };
    private void m0001_01b(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerSignedDefault(constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDefault(0x0/*constDefault*/);
        };
    private void m0001_01c(long[] rLongDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadLongSignedDelta(idx, source, rLongDictionary, rbB, rbMask, reader, rbRingBuffer)
            long tmpLng=(rLongDictionary[0x0/*idx*/] = (rLongDictionary[0x0/*source*/] + reader.readLongSigned()));
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        };
    private void m0001_01d(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedCopy(0xb/*target*/, 0xb/*source*/, rIntDictionary, reader);
        };
    private void m0001_01e(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDeltaOptional(0xc/*target*/, 0xc/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/);
        };
    private void m0001_01f(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            // Delta opp never uses PMAP
            long value = reader.readLongSigned();
            int result;
            if (0 == value) {
                rIntDictionary[0xd/*target*/] = 0;// set to absent
                result = 0x7fffffff/*constAbsent*/;
            } else {
                result = rIntDictionary[0xd/*target*/] = (int) (rIntDictionary[0xd/*source*/] + (value > 0 ? value - 1 : value));
            
            }
            rbB[rbMask & rbRingBuffer.addPos++] = result;
        };
    private void m0001_020(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x80000014/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0x1/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x5/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x5/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x5/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_021(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerSignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/);
        };
    private void m0001_022(long[] rLongDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadLongSignedDeltaOptional(idx, source, constAbsent, rLongDictionary, rbB, rbMask, reader, rbRingBuffer)
            long tmpLng=reader.readLongSignedDeltaOptional(0x1/*idx*/, 0x1/*source*/, rLongDictionary, 0x7fffffffffffffffL/*constAbsent*/);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        };
    private void m0001_023(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/, reader);
        };
    private void m0001_024(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x80000015/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0x1/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x6/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x6/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x6/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_025(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x80000016/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0x3/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x7/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x7/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x7/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_026(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x80000019/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0x1/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x8/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x8/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x8/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_027(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/, reader);
        };
    private void m0001_028(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x8000001a/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0xd/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x9/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x9/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x9/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_029(PrimitiveReader reader) {
            //genReadGroupClose(reader)
            reader.closePMap();
        };
    private void m0001_02a() {
            //genReadSequenceClose(backvalue)
            if (sequenceCountStackHead <= 0) {
                // no sequence to worry about or not the right time
                doSequence = false;
            } else {
            
                // each sequence will need to repeat the pmap but we only need to push
                // and pop the stack when the sequence is first encountered.
                // if count is zero we can pop it off but not until then.
            
                if (--sequenceCountStack[sequenceCountStackHead] < 1) {
                    // this group is a sequence so pop it off the stack.
                    --sequenceCountStackHead;
                    // finished this sequence so leave pointer where it is
                    jumpSequence = 0;
                } else {
                    // do this sequence again so move pointer back
                    jumpSequence = 0x16/*backvalue*/;
                }
                doSequence = true;
            }
        };


    private void grp9(PrimitiveReader reader, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, TextHeap textHeap, long[] rLongDictionary) {
        assert (gatherReadData(reader, activeScriptCursor));
        m0001_02b(reader);
        m0001_02c(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_02d(rbB,rbMask,reader,rbRingBuffer);
        m0001_02e(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_02f(rbB,rbMask,reader,rbRingBuffer);
        m0001_030(rbB,rbMask,rbRingBuffer);
        m0001_031(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_032(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_033(rbB,rbMask,reader,rbRingBuffer);
        m0001_034(rLongDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_035(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_036(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_037(rIntDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_038(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_039(rbB,rbMask,reader,rbRingBuffer);
        m0001_03a(rLongDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0001_03b(rbB,rbMask,reader,rbRingBuffer);
        m0001_03c(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_03d(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_03e(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_03f(rbB,rbMask,reader,rbRingBuffer);
        m0001_040(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_041(reader);
        m0001_042();
        activeScriptCursor=31;
    }
    private void m0001_02b(PrimitiveReader reader) {
            //genReadGroupPMapOpen(nonTemplatePMapSize, reader)
            reader.openPMap(0x3/*nonTemplatePMapSize*/);
        };
    private void m0001_02c(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedCopy(0x4/*target*/, 0x4/*source*/, rIntDictionary, reader);
        };
    private void m0001_02d(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/, reader);
        };
    private void m0001_02e(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIICopy(idx, rbB, rbMask, reader, textHeap, rbRingBuffer)
             int len;
            if (PrimitiveReader.popPMapBit(reader)!=0) {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    len=StaticGlue.readASCIIToHeapValue(val, tmp, 0x4/*idx*/, textHeap, reader);
                } else {
                    len=StaticGlue.readASCIIToHeapNone(0x4/*idx*/, val, textHeap, reader);
                }
            } else {
                len = textHeap.valueLength(0x4/*idx*/);
            }
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x4/*idx*/, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
        };
    private void m0001_02f(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedOptional(constAbsent, rbB, rbMask, reader, rbRingBuffer)
            int xi1;
            rbB[rbMask & rbRingBuffer.addPos++] = 0 == (xi1 = reader.readIntegerUnsigned()) ? 0x7fffffff/*constAbsent*/ : xi1 - 1;
        };
    private void m0001_030(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedConstant(constDefault, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x9/*constDefault*/;
        };
    private void m0001_031(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedCopy(0x8/*target*/, 0x8/*source*/, rIntDictionary, reader);
        };
    private void m0001_032(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedIncrement(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedIncrement(0x9/*target*/, 0x9/*source*/, rIntDictionary);
        };
    private void m0001_033(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerSignedDefault(constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDefault(0x0/*constDefault*/);
        };
    private void m0001_034(long[] rLongDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadLongSignedDelta(idx, source, rLongDictionary, rbB, rbMask, reader, rbRingBuffer)
            long tmpLng=(rLongDictionary[0x0/*idx*/] = (rLongDictionary[0x0/*source*/] + reader.readLongSigned()));
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        };
    private void m0001_035(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedCopy(0xb/*target*/, 0xb/*source*/, rIntDictionary, reader);
        };
    private void m0001_036(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDeltaOptional(0xc/*target*/, 0xc/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/);
        };
    private void m0001_037(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer)
            // Delta opp never uses PMAP
            long value = reader.readLongSigned();
            int result;
            if (0 == value) {
                rIntDictionary[0xd/*target*/] = 0;// set to absent
                result = 0x7fffffff/*constAbsent*/;
            } else {
                result = rIntDictionary[0xd/*target*/] = (int) (rIntDictionary[0xd/*source*/] + (value > 0 ? value - 1 : value));
            
            }
            rbB[rbMask & rbRingBuffer.addPos++] = result;
        };
    private void m0001_038(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x80000014/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0x1/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x5/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x5/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x5/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_039(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerSignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/);
        };
    private void m0001_03a(long[] rLongDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadLongSignedDeltaOptional(idx, source, constAbsent, rLongDictionary, rbB, rbMask, reader, rbRingBuffer)
            long tmpLng=reader.readLongSignedDeltaOptional(0x1/*idx*/, 0x1/*source*/, rLongDictionary, 0x7fffffffffffffffL/*constAbsent*/);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        };
    private void m0001_03b(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/, reader);
        };
    private void m0001_03c(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x80000015/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0x1/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x6/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x6/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x6/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_03d(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x80000016/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0x3/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x7/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x7/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x7/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_03e(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x80000019/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0x1/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x8/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x8/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x8/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_03f(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/, reader);
        };
    private void m0001_040(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defIdx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = 0x8000001a/*defIdx*/;
                rbB[rbMask & rbRingBuffer.addPos++] = 0xd/*defLen*/;
            } else {
                byte val;
                int tmp;
                if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                    tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0x9/*idx*/, textHeap, reader);
                } else {
                    tmp=StaticGlue.readASCIIToHeapNone(0x9/*idx*/, val, textHeap, reader);
                }
                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0x9/*idx*/, tmp, textHeap);
                rbB[rbMask & rbRingBuffer.addPos++] = tmp;
            }
        };
    private void m0001_041(PrimitiveReader reader) {
            //genReadGroupClose(reader)
            reader.closePMap();
        };
    private void m0001_042() {
            //genReadSequenceClose(backvalue)
            if (sequenceCountStackHead <= 0) {
                // no sequence to worry about or not the right time
                doSequence = false;
            } else {
            
                // each sequence will need to repeat the pmap but we only need to push
                // and pop the stack when the sequence is first encountered.
                // if count is zero we can pop it off but not until then.
            
                if (--sequenceCountStack[sequenceCountStackHead] < 1) {
                    // this group is a sequence so pop it off the stack.
                    --sequenceCountStackHead;
                    // finished this sequence so leave pointer where it is
                    jumpSequence = 0;
                } else {
                    // do this sequence again so move pointer back
                    jumpSequence = 0x16/*backvalue*/;
                }
                doSequence = true;
            }
        };


    private void grp32(int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, int[] rIntDictionary, TextHeap textHeap, long[] rLongDictionary) {
        assert (gatherReadData(reader, activeScriptCursor));
        m0002_043(rbB,rbMask,rbRingBuffer);
        m0002_044(rbB,rbMask,rbRingBuffer);
        m0002_045(rbB,rbMask,rbRingBuffer);
        m0002_046(rbB,rbMask,reader,rIntDictionary,rbRingBuffer);
        m0002_047(rbB,rbMask,reader,rIntDictionary,rbRingBuffer);
        m0002_048(rbB,rbMask,reader,textHeap,rbRingBuffer);
        if (m0002_049(rbB,rbMask,rbRingBuffer,rIntDictionary,reader)) {return;};
        m0002_04a(reader);
        m0002_04b(rbB,rbMask,rbRingBuffer);
        m0002_04c(rbB,rbMask,reader,rbRingBuffer);
        m0002_04d(rbB,rbMask,reader,rbRingBuffer);
        m0002_04e(rLongDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0002_04f(rbB,rbMask,reader,rbRingBuffer);
        m0002_050(rbB,rbMask,reader,rIntDictionary,rbRingBuffer);
        m0002_051(rbB,rbMask,rbRingBuffer);
        m0002_052(reader);
        m0002_053();
        activeScriptCursor=47;
    }
    private void m0002_043(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x80000027/*constIdx*/;
            rbB[rbMask & rbRingBuffer.addPos++] = 0x5/*constLen*/;
        };
    private void m0002_044(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x80000000/*constIdx*/;
            rbB[rbMask & rbRingBuffer.addPos++] = 0x0/*constLen*/;
        };
    private void m0002_045(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x80000000/*constIdx*/;
            rbB[rbMask & rbRingBuffer.addPos++] = 0x0/*constLen*/;
        };
    private void m0002_046(int[] rbB,int rbMask,PrimitiveReader reader,int[] rIntDictionary,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsigned(target, rbB, rbMask, reader, rIntDictionary, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[0x11/*target*/] = reader.readIntegerUnsigned();
        };
    private void m0002_047(int[] rbB,int rbMask,PrimitiveReader reader,int[] rIntDictionary,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsigned(target, rbB, rbMask, reader, rIntDictionary, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[0x12/*target*/] = reader.readIntegerUnsigned();
        };
    private void m0002_048(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIINone(idx, rbB, rbMask, reader, textHeap, rbRingBuffer)
            byte val;
            int tmp;
            if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                tmp=StaticGlue.readASCIIToHeapValue(val, tmp, 0xd/*idx*/, textHeap, reader);
            } else {
                tmp=StaticGlue.readASCIIToHeapNone(0xd/*idx*/, val, textHeap, reader);
            }
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(0xd/*idx*/, tmp, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = tmp;
        };
    private boolean m0002_049(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer,int[] rIntDictionary,PrimitiveReader reader) {
            //genReadLength(target, jumpToTarget, rbB, rbMask, rbRingBuffer, rIntDictionary, reader)
            int length;
            int value = rIntDictionary[0x13/*target*/] = length = reader.readIntegerUnsigned();
            rbB[rbMask & rbRingBuffer.addPos++] = value;
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                activeScriptCursor = 0x2f/*jumpToTarget*/;
                return true;
            } else {
                // jumpSequence = 0;
                sequenceCountStack[++sequenceCountStackHead] = length;
                return false;
           }
        };
    private void m0002_04a(PrimitiveReader reader) {
            //genReadGroupPMapOpen(nonTemplatePMapSize, reader)
            reader.openPMap(0x3/*nonTemplatePMapSize*/);
        };
    private void m0002_04b(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x80000000/*constIdx*/;
            rbB[rbMask & rbRingBuffer.addPos++] = 0x0/*constLen*/;
        };
    private void m0002_04c(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadLongUnsignedOptional(constAbsent, rbB, rbMask, reader, rbRingBuffer)
            long value = reader.readLongUnsigned();
            long tmpLng=value == 0 ? 0x7fffffffffffffffL/*constAbsent*/ : value - 1;
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        };
    private void m0002_04d(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/, reader);
        };
    private void m0002_04e(long[] rLongDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadLongUnsignedNone(idx, rLongDictionary, rbB, rbMask, reader, rbRingBuffer)
            long tmpLng=rLongDictionary[0x3/*idx*/] = reader.readLongUnsigned();
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        };
    private void m0002_04f(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefault(constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefault(0x1/*constDefault*/);
        };
    private void m0002_050(int[] rbB,int rbMask,PrimitiveReader reader,int[] rIntDictionary,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsigned(target, rbB, rbMask, reader, rIntDictionary, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[0x16/*target*/] = reader.readIntegerUnsigned();
        };
    private void m0002_051(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedConstant(constDefault, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x9/*constDefault*/;
        };
    private void m0002_052(PrimitiveReader reader) {
            //genReadGroupClose(reader)
            reader.closePMap();
        };
    private void m0002_053() {
            //genReadSequenceClose(backvalue)
            if (sequenceCountStackHead <= 0) {
                // no sequence to worry about or not the right time
                doSequence = false;
            } else {
            
                // each sequence will need to repeat the pmap but we only need to push
                // and pop the stack when the sequence is first encountered.
                // if count is zero we can pop it off but not until then.
            
                if (--sequenceCountStack[sequenceCountStackHead] < 1) {
                    // this group is a sequence so pop it off the stack.
                    --sequenceCountStackHead;
                    // finished this sequence so leave pointer where it is
                    jumpSequence = 0;
                } else {
                    // do this sequence again so move pointer back
                    jumpSequence = 0x8/*backvalue*/;
                }
                doSequence = true;
            }
        };


    private void grp39(PrimitiveReader reader, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, long[] rLongDictionary, int[] rIntDictionary) {
        assert (gatherReadData(reader, activeScriptCursor));
        m0002_054(reader);
        m0002_055(rbB,rbMask,rbRingBuffer);
        m0002_056(rbB,rbMask,reader,rbRingBuffer);
        m0002_057(rbB,rbMask,reader,rbRingBuffer);
        m0002_058(rLongDictionary,rbB,rbMask,reader,rbRingBuffer);
        m0002_059(rbB,rbMask,reader,rbRingBuffer);
        m0002_05a(rbB,rbMask,reader,rIntDictionary,rbRingBuffer);
        m0002_05b(rbB,rbMask,rbRingBuffer);
        m0002_05c(reader);
        m0002_05d();
        activeScriptCursor=47;
    }
    private void m0002_054(PrimitiveReader reader) {
            //genReadGroupPMapOpen(nonTemplatePMapSize, reader)
            reader.openPMap(0x3/*nonTemplatePMapSize*/);
        };
    private void m0002_055(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x80000000/*constIdx*/;
            rbB[rbMask & rbRingBuffer.addPos++] = 0x0/*constLen*/;
        };
    private void m0002_056(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadLongUnsignedOptional(constAbsent, rbB, rbMask, reader, rbRingBuffer)
            long value = reader.readLongUnsigned();
            long tmpLng=value == 0 ? 0x7fffffffffffffffL/*constAbsent*/ : value - 1;
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        };
    private void m0002_057(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/, reader);
        };
    private void m0002_058(long[] rLongDictionary,int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadLongUnsignedNone(idx, rLongDictionary, rbB, rbMask, reader, rbRingBuffer)
            long tmpLng=rLongDictionary[0x3/*idx*/] = reader.readLongUnsigned();
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        };
    private void m0002_059(int[] rbB,int rbMask,PrimitiveReader reader,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedDefault(constDefault, rbB, rbMask, reader, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefault(0x1/*constDefault*/);
        };
    private void m0002_05a(int[] rbB,int rbMask,PrimitiveReader reader,int[] rIntDictionary,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsigned(target, rbB, rbMask, reader, rIntDictionary, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[0x16/*target*/] = reader.readIntegerUnsigned();
        };
    private void m0002_05b(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadIntegerUnsignedConstant(constDefault, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x9/*constDefault*/;
        };
    private void m0002_05c(PrimitiveReader reader) {
            //genReadGroupClose(reader)
            reader.closePMap();
        };
    private void m0002_05d() {
            //genReadSequenceClose(backvalue)
            if (sequenceCountStackHead <= 0) {
                // no sequence to worry about or not the right time
                doSequence = false;
            } else {
            
                // each sequence will need to repeat the pmap but we only need to push
                // and pop the stack when the sequence is first encountered.
                // if count is zero we can pop it off but not until then.
            
                if (--sequenceCountStack[sequenceCountStackHead] < 1) {
                    // this group is a sequence so pop it off the stack.
                    --sequenceCountStackHead;
                    // finished this sequence so leave pointer where it is
                    jumpSequence = 0;
                } else {
                    // do this sequence again so move pointer back
                    jumpSequence = 0x8/*backvalue*/;
                }
                doSequence = true;
            }
        };


    private void grp0(int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        assert (gatherReadData(reader, activeScriptCursor));
        m0099_05e(rbB,rbMask,rbRingBuffer);
        activeScriptCursor=1;
    }
    private void m0099_05e(int[] rbB,int rbMask,FASTRingBuffer rbRingBuffer) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask, rbRingBuffer)
            rbB[rbMask & rbRingBuffer.addPos++] = 0x80000000/*constIdx*/;
            rbB[rbMask & rbRingBuffer.addPos++] = 0x2/*constLen*/;
        };

    public boolean dispatchReadByToken() {
        doSequence = false;
        int x = activeScriptCursor;
        if ((x&0x20)==0) {
            if ((x&0x8)==0) {
                if ((x&0x1)==0) {
                    assert(0==x) : "found value of "+x;
                    grp0(rbB, rbMask, rbRingBuffer);
                } else {
                    assert(1==x) : "found value of "+x;
                    grp1(textHeap, rLongDictionary, rLongInit, rIntDictionary, rIntInit, rbB, rbMask, rbRingBuffer, reader);
                }
            } else {
                assert(9==x) : "found value of "+x;
                grp9(reader, rIntDictionary, rbB, rbMask, rbRingBuffer, textHeap, rLongDictionary);
            }
        } else {
            if ((x&0x1)==0) {
                assert(32==x) : "found value of "+x;
                grp32(rbB, rbMask, rbRingBuffer, reader, rIntDictionary, textHeap, rLongDictionary);
            } else {
                assert(39==x) : "found value of "+x;
                grp39(reader, rbB, rbMask, rbRingBuffer, rLongDictionary, rIntDictionary);
            }
        }
        return doSequence;
    }

}
