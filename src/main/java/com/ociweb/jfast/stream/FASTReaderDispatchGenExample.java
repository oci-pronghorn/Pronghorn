package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.field.StaticGlue;

public class FASTReaderDispatchGenExample extends FASTReaderDispatch {

    public FASTReaderDispatchGenExample(PrimitiveReader reader, DictionaryFactory dcr, int nonTemplatePMapSize,
            int[][] dictionaryMembers, int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript,
            int maxNestedGroupDepth, int ringBits, int ringTextBits) {
        super(reader, dcr, nonTemplatePMapSize, dictionaryMembers, maxTextLen, maxVectorLen, charGap, bytesGap,
                fullScript, maxNestedGroupDepth, ringBits, ringTextBits);
    }

    final int constIntAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
    final long constLongAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;

    // TODO: B, this code generation must take place when loading the catalog
    // binary file. So catalog file can be the same cross languages.
    // TODO: B, copy other dispatch and use it for code generation, if possible
    // build generator that makes use of its own source as template.

    public boolean dispatchReadByToken() { //TODO: A, optimize this in generator?
        //000000
        //000001
        //001001
        //100000
        int c = activeScriptCursor; //TODO: must build tree builder for code generation.
        if (0 == (c & 1)) {
            //0 or 32
            if (0==c) {
                case0();
                activeScriptCursor = 1; //jumping to end of 0
                return false;
            } else {
                doSequence = false;
                //TODO: X, by counting the entry points this can be re-written to optimize for situations.
                
                //TODO: remove mantDictionary next and pull up each member to param here so they are on the stack.
                case32();
//                
//                int length = rIntDictionary[0x11/*target*/];
//                if (length == 0) {
//                    // jumping over sequence (forward) it was skipped (rare case)
//                    activeScriptCursor = 47;
//                } else {
//                    sequenceCountStack[++sequenceCountStackHead] = length;
//                }
//                
//                
//                case39();
                
                activeScriptCursor = 47;
                return doSequence;
            }
            
        } else {
            //1 or 9
            if (1==c) {
                doSequence = false;//must always set first.
                case1();
//                int length = rIntDictionary[0x3/*target*/];
//
//                
//                if (length == 0) {
//                    // jumping over sequence (forward) it was skipped (rare case)
//                    activeScriptCursor = 31; //Jumping to END of 1?
//                } else {
//                    sequenceCountStack[++sequenceCountStackHead] = length;
//                }
//                
//                //TODO: in generated code if length is zero must jump out and skip over the next block.
//                
//                
//                case9(); //mantDictionary inserted in place of long dictionary
                activeScriptCursor = 31; // +1 +1 //jumping to end of 1
                return doSequence;
            } else {
                doSequence = false;//must always set first.
                
                case9(); //mantDictionary inserted in place of long dictionary
                activeScriptCursor = 31;//jumping to end of 1
                return doSequence;
            }
            
            
        }
        
        //TODO: each case method will take needed members as args
        //TODO: must reduce size of large methods as well.
        
//        
//        switch (activeScriptCursor) {
//            case 0:
//            return top0();
//            case 1:
//            return top1();
//            case 9:     
//            return top9();
//
//            case 32://30:
//            return top32();
//            default:
//                assert (false) : "Unsupported Template";
//                return false;
//        }
    }

    private void case1() {     // for message 1
        int[] rbB = this.rbB;
        test1(rbB);
        if (m0001_012(rbB)) {return;};
        test2(rbB);
    }

    private void test1(int[] rbB) {
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
        m0001_00c(rbB,rbMask);
        m0001_00d(rbB,rbMask);
        m0001_00e(rbB,rbMask);
        m0001_00f(rbB);
        m0001_010(rbB);
        m0001_011(rbB);
    }

    private void test2(int[] rbB) {
        m0001_013(reader);
        m0001_014(rIntDictionary,rbB,rbMask,reader);
        m0001_015(rbB,rbMask,reader);
        m0001_016(rbB);
        m0001_017(rbB,rbMask,reader);
        m0001_018(rbB);
        m0001_019(rIntDictionary,rbB,rbMask,reader);
        m0001_01a(rIntDictionary,rbB);
        m0001_01b(rbB);
        m0001_01c(rLongDictionary,rbB);
        m0001_01d(rIntDictionary,rbB,rbMask,reader);
        m0001_01e(rIntDictionary,rbB);
        m0001_01f(rIntDictionary,rbB,rbMask,reader);
        m0001_020(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_021(rbB);
        m0001_022(rLongDictionary,rbB);
        m0001_023(rbB,rbMask,reader);
        m0001_024(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_025(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_026(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_027(rbB,rbMask,reader);
        m0001_028(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_029(reader);
        m0001_02a();
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
    private void m0001_00c(int[] rbB,int rbMask) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x80000001/*constIdx*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x3/*constLen*/);
        
    };
    private void m0001_00d(int[] rbB,int rbMask) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x80000002/*constIdx*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x1/*constLen*/);
        
    };
    private void m0001_00e(int[] rbB,int rbMask) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x80000003/*constIdx*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0xd/*constLen*/);
        
    };
    private void m0001_00f(int[] rbB) {
            //genReadIntegerUnsigned(target, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x0/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0001_010(int[] rbB) {
            //genReadIntegerUnsigned(target, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x1/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0001_011(int[] rbB) {
            //genReadIntegerUnsigned(target, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x2/*target*/] = reader.readIntegerUnsigned());
        
    };
    private boolean m0001_012(int[] rbB) {
            //genReadLength(target, jumpToTarget, rbB)
            int length;
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x3/*target*/] = length = reader.readIntegerUnsigned());
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
    private void m0001_014(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedCopy(0x4/*target*/, 0x4/*source*/, rIntDictionary));
        
    };
    private void m0001_015(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_016(int[] rbB) {
            //genReadASCIICopy(idx, rbB)
            {
             int len;
            if (reader.popPMapBit()!=0) {
                len = StaticGlue.readASCIIToHeap(0x4/*idx*/, textHeap, reader);
            } else {
                len = textHeap.valueLength(0x4/*idx*/);
            }
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x4/*idx*/, len));
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_017(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedOptional(constAbsent, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.staticReadIntegerUnsignedOptional(0x7fffffff/*constAbsent*/, reader));
        
    };
    private void m0001_018(int[] rbB) {
            //genReadIntegerUnsignedConstant(constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x9/*constDefault*/);
        
    };
    private void m0001_019(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedCopy(0x8/*target*/, 0x8/*source*/, rIntDictionary));
        
    };
    private void m0001_01a(int[] rIntDictionary,int[] rbB) {
            //genReadIntegerUnsignedIncrement(target, source, rIntDictionary, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedIncrement(0x9/*target*/, 0x9/*source*/, rIntDictionary));
        
    };
    private void m0001_01b(int[] rbB) {
            //genReadIntegerSignedDefault(constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerSignedDefault(0x0/*constDefault*/));
        
    };
    private void m0001_01c(long[] rLongDictionary,int[] rbB) {
            //genReadLongSignedDelta(idx, source, rLongDictionary, rbB)
            {
            long tmpLng=reader.readLongSignedDelta(0x0/*idx*/, 0x0/*source*/, rLongDictionary);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0001_01d(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedCopy(0xb/*target*/, 0xb/*source*/, rIntDictionary));
        
    };
    private void m0001_01e(int[] rIntDictionary,int[] rbB) {
            //genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerSignedDeltaOptional(0xc/*target*/, 0xc/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_01f(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDeltaOptional(0xd/*target*/, 0xd/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_020(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x5/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x1/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x5/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x5/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_021(int[] rbB) {
            //genReadIntegerSignedDefaultOptional(constAbsent, constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerSignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_022(long[] rLongDictionary,int[] rbB) {
            //genReadLongSignedDeltaOptional(idx, source, constAbsent, rLongDictionary, rbB)
            {
            long tmpLng=reader.readLongSignedDeltaOptional(0x1/*idx*/, 0x1/*source*/, rLongDictionary, 0x7fffffffffffffffL/*constAbsent*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0001_023(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_024(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x6/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x1/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x6/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x6/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_025(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x7/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x3/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x7/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x7/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_026(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x8/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x1/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x8/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x8/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_027(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_028(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x9/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0xd/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x9/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x9/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
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


    private void case9() {     // for message 1
        int[] rbB = this.rbB;
        assert (gatherReadData(reader, activeScriptCursor));
        m0001_02b(reader);
        m0001_02c(rIntDictionary,rbB,rbMask,reader);
        m0001_02d(rbB,rbMask,reader);
        m0001_02e(rbB);
        m0001_02f(rbB,rbMask,reader);
        m0001_030(rbB);
        m0001_031(rIntDictionary,rbB,rbMask,reader);
        m0001_032(rIntDictionary,rbB);
        m0001_033(rbB);
        m0001_034(rLongDictionary,rbB);
        m0001_035(rIntDictionary,rbB,rbMask,reader);
        m0001_036(rIntDictionary,rbB);
        m0001_037(rIntDictionary,rbB,rbMask,reader);
        m0001_038(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_039(rbB);
        m0001_03a(rLongDictionary,rbB);
        m0001_03b(rbB,rbMask,reader);
        m0001_03c(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_03d(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_03e(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_03f(rbB,rbMask,reader);
        m0001_040(rbB,rbMask,reader,textHeap,rbRingBuffer);
        m0001_041(reader);
        m0001_042();
    }
    private void m0001_02b(PrimitiveReader reader) {
            //genReadGroupPMapOpen(nonTemplatePMapSize, reader)
            reader.openPMap(0x3/*nonTemplatePMapSize*/);
        
    };
    private void m0001_02c(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedCopy(0x4/*target*/, 0x4/*source*/, rIntDictionary));
        
    };
    private void m0001_02d(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_02e(int[] rbB) {
            //genReadASCIICopy(idx, rbB)
            {
             int len;
            if (reader.popPMapBit()!=0) {
                len = StaticGlue.readASCIIToHeap(0x4/*idx*/, textHeap, reader);
            } else {
                len = textHeap.valueLength(0x4/*idx*/);
            }
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x4/*idx*/, len));
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_02f(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedOptional(constAbsent, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.staticReadIntegerUnsignedOptional(0x7fffffff/*constAbsent*/, reader));
        
    };
    private void m0001_030(int[] rbB) {
            //genReadIntegerUnsignedConstant(constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x9/*constDefault*/);
        
    };
    private void m0001_031(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedCopy(0x8/*target*/, 0x8/*source*/, rIntDictionary));
        
    };
    private void m0001_032(int[] rIntDictionary,int[] rbB) {
            //genReadIntegerUnsignedIncrement(target, source, rIntDictionary, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedIncrement(0x9/*target*/, 0x9/*source*/, rIntDictionary));
        
    };
    private void m0001_033(int[] rbB) {
            //genReadIntegerSignedDefault(constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerSignedDefault(0x0/*constDefault*/));
        
    };
    private void m0001_034(long[] rLongDictionary,int[] rbB) {
            //genReadLongSignedDelta(idx, source, rLongDictionary, rbB)
            {
            long tmpLng=reader.readLongSignedDelta(0x0/*idx*/, 0x0/*source*/, rLongDictionary);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0001_035(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedCopy(0xb/*target*/, 0xb/*source*/, rIntDictionary));
        
    };
    private void m0001_036(int[] rIntDictionary,int[] rbB) {
            //genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerSignedDeltaOptional(0xc/*target*/, 0xc/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_037(int[] rIntDictionary,int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDeltaOptional(0xd/*target*/, 0xd/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_038(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x5/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x1/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x5/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x5/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_039(int[] rbB) {
            //genReadIntegerSignedDefaultOptional(constAbsent, constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerSignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_03a(long[] rLongDictionary,int[] rbB) {
            //genReadLongSignedDeltaOptional(idx, source, constAbsent, rLongDictionary, rbB)
            {
            long tmpLng=reader.readLongSignedDeltaOptional(0x1/*idx*/, 0x1/*source*/, rLongDictionary, 0x7fffffffffffffffL/*constAbsent*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0001_03b(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_03c(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x6/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x1/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x6/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x6/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_03d(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x7/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x3/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x7/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x7/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_03e(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x8/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x1/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x8/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x8/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_03f(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_040(int[] rbB,int rbMask,PrimitiveReader reader,TextHeap textHeap,FASTRingBuffer rbRingBuffer) {
            //genReadASCIIDefault(idx, defLen, rbB, rbMask, reader, textHeap, rbRingBuffer)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x9/*idx*/);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0xd/*defLen*/);
            } else {
                int len = StaticGlue.readASCIIToHeap(0x9/*idx*/, textHeap, reader);
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x9/*idx*/, len));
                FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
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


    private void case32() {     // for message 2
        int[] rbB = this.rbB;
        assert (gatherReadData(reader, activeScriptCursor));
        m0002_043(rbB,rbMask);
        m0002_044(rbB,rbMask);
        m0002_045(rbB,rbMask);
        m0002_046(rbB);
        m0002_047(rbB);
        m0002_048(rbB);
        if (m0002_049(rbB)) {return;};
        m0002_04a(reader);
        m0002_04b(rbB,rbMask);
        m0002_04c(rbB);
        m0002_04d(rbB,rbMask,reader);
        m0002_04e(rLongDictionary,rbB);
        m0002_04f(rbB);
        m0002_050(rbB);
        m0002_051(rbB);
        m0002_052(reader);
        m0002_053();
    }
    private void m0002_043(int[] rbB,int rbMask) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x8000000a/*constIdx*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x5/*constLen*/);
        
    };
    private void m0002_044(int[] rbB,int rbMask) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x8000000b/*constIdx*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x0/*constLen*/);
        
    };
    private void m0002_045(int[] rbB,int rbMask) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x8000000c/*constIdx*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x0/*constLen*/);
        
    };
    private void m0002_046(int[] rbB) {
            //genReadIntegerUnsigned(target, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x11/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_047(int[] rbB) {
            //genReadIntegerUnsigned(target, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x12/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_048(int[] rbB) {
            //genReadASCIINone(idx, rbB)
            {
            int len = StaticGlue.readASCIIToHeap(0xd/*idx*/, textHeap, reader);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0xd/*idx*/, len));
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private boolean m0002_049(int[] rbB) {
            //genReadLength(target, jumpToTarget, rbB)
            int length;
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x13/*target*/] = length = reader.readIntegerUnsigned());
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
    private void m0002_04b(int[] rbB,int rbMask) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x8000000e/*constIdx*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x0/*constLen*/);
        
    };
    private void m0002_04c(int[] rbB) {
            //genReadLongUnsignedOptional(constAbsent, rbB)
            {
            long tmpLng=reader.readLongUnsignedOptional(0x7fffffffffffffffL/*constAbsent*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0002_04d(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0002_04e(long[] rLongDictionary,int[] rbB) {
            //genReadLongUnsignedNone(idx, rLongDictionary, rbB)
            {
            long tmpLng=reader.readLongUnsigned(0x3/*idx*/, rLongDictionary);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0002_04f(int[] rbB) {
            //genReadIntegerUnsignedDefault(constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefault(0x1/*constDefault*/));
        
    };
    private void m0002_050(int[] rbB) {
            //genReadIntegerUnsigned(target, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x16/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_051(int[] rbB) {
            //genReadIntegerUnsignedConstant(constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x9/*constDefault*/);
        
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


    private void case39() {     // for message 2
        int[] rbB = this.rbB;
        assert (gatherReadData(reader, activeScriptCursor));
        m0002_054(reader);
        m0002_055(rbB,rbMask);
        m0002_056(rbB);
        m0002_057(rbB,rbMask,reader);
        m0002_058(rLongDictionary,rbB);
        m0002_059(rbB);
        m0002_05a(rbB);
        m0002_05b(rbB);
        m0002_05c(reader);
        m0002_05d();
    }
    private void m0002_054(PrimitiveReader reader) {
            //genReadGroupPMapOpen(nonTemplatePMapSize, reader)
            reader.openPMap(0x3/*nonTemplatePMapSize*/);
        
    };
    private void m0002_055(int[] rbB,int rbMask) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x8000000e/*constIdx*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x0/*constLen*/);
        
    };
    private void m0002_056(int[] rbB) {
            //genReadLongUnsignedOptional(constAbsent, rbB)
            {
            long tmpLng=reader.readLongUnsignedOptional(0x7fffffffffffffffL/*constAbsent*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0002_057(int[] rbB,int rbMask,PrimitiveReader reader) {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0002_058(long[] rLongDictionary,int[] rbB) {
            //genReadLongUnsignedNone(idx, rLongDictionary, rbB)
            {
            long tmpLng=reader.readLongUnsigned(0x3/*idx*/, rLongDictionary);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0002_059(int[] rbB) {
            //genReadIntegerUnsignedDefault(constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefault(0x1/*constDefault*/));
        
    };
    private void m0002_05a(int[] rbB) {
            //genReadIntegerUnsigned(target, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x16/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_05b(int[] rbB) {
            //genReadIntegerUnsignedConstant(constDefault, rbB)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x9/*constDefault*/);
        
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


    private void case0() {     // for message 99
        assert (gatherReadData(reader, activeScriptCursor));
        m0099_05e(rbB,rbMask);
    }
    private void m0099_05e(int[] rbB,int rbMask) {
            //genReadTextConstant(constIdx, constLen, rbB, rbMask)
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x80000000/*constIdx*/);
            FASTRingBuffer.appendi(rbB, rbRingBuffer.addPos++, rbMask, 0x2/*constLen*/);
        
    };
}
