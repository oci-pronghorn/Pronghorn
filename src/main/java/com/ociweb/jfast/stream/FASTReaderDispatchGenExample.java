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
                case1();
                int length = rIntDictionary[0x3/*target*/];

                
                if (length == 0) {
                    // jumping over sequence (forward) it was skipped (rare case)
                    activeScriptCursor = 31; //Jumping to END of 1?
                } else {
                    sequenceCountStack[++sequenceCountStackHead] = length;
                }
                
                //TODO: in generated code if length is zero must jump out and skip over the next block.
                
                doSequence = false;//must always set first.
                
                case9(); //mantDictionary inserted in place of long dictionary
                activeScriptCursor = 31; // +1 +1 //jumping to end of 1
                return doSequence;
            } else {
                doSequence = false;//must always set first.
                
                case9(); //mantDictionary inserted in place of long dictionary
                activeScriptCursor = 31;//jumping to end of 1
                return doSequence;
            }
            
            
        }
        
        //TODO: urgent must not replace mantDictionary
        
        
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

    

    
    
    
    //NOTE:everything generated after this point.
    
    private void case1() {     // for message 1
        assert (gatherReadData(reader, activeScriptCursor));
        m0001_001();
        m0001_002();
        m0001_003();
        m0001_004();
        m0001_005();
        m0001_006();
        m0001_007();
        m0001_008();
        m0001_009();
        m0001_00a();
        m0001_00b();
        m0001_00c();
        m0001_00d();
        m0001_00e();
        m0001_00f();
        m0001_010();
    }
    private void m0001_001() {
            //genReadDictionaryDecimalReset(idx)
            expDictionary[0x0/*idx*/] = expInit[0x0/*idx*/];
            mantDictionary[0x0/*idx*/] = mantInit[0x0/*idx*/];
        
    };
    private void m0001_002() {
            //genReadDictionaryDecimalReset(idx)
            expDictionary[0x1/*idx*/] = expInit[0x1/*idx*/];
            mantDictionary[0x1/*idx*/] = mantInit[0x1/*idx*/];
        
    };
    private void m0001_003() {
            //genReadDictionaryTextReset(idx)
            charDictionary.reset(0x4/*idx*/);
        
    };
    private void m0001_004() {
            //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0x4/*idx*/] = rIntInit[0x4/*idx*/];
        
    };
    private void m0001_005() {
            //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0x8/*idx*/] = rIntInit[0x8/*idx*/];
        
    };
    private void m0001_006() {
            //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0x9/*idx*/] = rIntInit[0x9/*idx*/];
        
    };
    private void m0001_007() {
            //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0xa/*idx*/] = rIntInit[0xa/*idx*/];
        
    };
    private void m0001_008() {
            //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0xb/*idx*/] = rIntInit[0xb/*idx*/];
        
    };
    private void m0001_009() {
            //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0xc/*idx*/] = rIntInit[0xc/*idx*/];
        
    };
    private void m0001_00a() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x80000001/*constIdx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x3/*constLen*/);
        
    };
    private void m0001_00b() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x80000002/*constIdx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x1/*constLen*/);
        
    };
    private void m0001_00c() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x80000003/*constIdx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0xd/*constLen*/);
        
    };
    private void m0001_00d() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, rIntDictionary[0x0/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0001_00e() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, rIntDictionary[0x1/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0001_00f() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, rIntDictionary[0x2/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0001_010() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, rIntDictionary[0x3/*target*/] = reader.readIntegerUnsigned());
        
    };


    private void case9() {     // for message 1
        assert (gatherReadData(reader, activeScriptCursor));
        m0001_011();
        m0001_012();
        m0001_013();
        m0001_014();
        m0001_015();
        m0001_016();
        m0001_017();
        m0001_018();
        m0001_019();
        m0001_01a();
        m0001_01b();
        m0001_01c();
        m0001_01d();
        m0001_01e();
        m0001_01f();
        m0001_020();
        m0001_021();
        m0001_022();
        m0001_023();
        m0001_024();
        m0001_025();
        m0001_026();
        m0001_027();
        m0001_028();
    }
    private void m0001_011() {
            //genReadGroupPMapOpen()
            reader.openPMap(nonTemplatePMapSize);
        
    };
    private void m0001_012() {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedCopy(0x4/*target*/, 0x4/*source*/, rIntDictionary));
        
    };
    private void m0001_013() {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_014() {
            //genReadASCIICopy(idx)
            {
            if (reader.popPMapBit()!=0) {
                StaticGlue.readASCIIToHeap(0x4/*idx*/, charDictionary, reader);
            }
            int len = charDictionary.valueLength(0x4/*idx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, queue.writeTextToRingBuffer(0x4/*idx*/, len));
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, len);
            }
        
    };
    private void m0001_015() {
            //genReadIntegerUnsignedOptional(constAbsent)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, StaticGlue.staticReadIntegerUnsignedOptional(0x7fffffff/*constAbsent*/, reader));
        
    };
    private void m0001_016() {
            //genReadIntegerUnsignedConstant(constDefault)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x9/*constDefault*/);
        
    };
    private void m0001_017() {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedCopy(0x8/*target*/, 0x8/*source*/, rIntDictionary));
        
    };
    private void m0001_018() {
            //genReadIntegerUnsignedIncrement(target, source, rIntDictionary)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedIncrement(0x9/*target*/, 0x9/*source*/, rIntDictionary));
        
    };
    private void m0001_019() {
            //genReadIntegerSignedDefault(constDefault)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerSignedDefault(0x0/*constDefault*/));
        
    };
    private void m0001_01a() {
            //genReadLongSignedDelta(idx, source, rLongDictionary)
            {
            long tmpLng=reader.readLongSignedDelta(0x0/*idx*/, 0x0/*source*/, mantDictionary);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0001_01b() {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedCopy(0xa/*target*/, 0xa/*source*/, rIntDictionary));
        
    };
    private void m0001_01c() {
            //genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerSignedDeltaOptional(0xb/*target*/, 0xb/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_01d() {
            //genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedDeltaOptional(0xc/*target*/, 0xc/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_01e() {
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, StaticGlue.INIT_VALUE_MASK | 0x5/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x1/*defLen*/);
            } else {
                StaticGlue.readASCIIToHeap(0x5/*idx*/, charDictionary, reader);
                int len = charDictionary.valueLength(0x5/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, queue.writeTextToRingBuffer(0x5/*idx*/, len));
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, len);
            }
        
    };
    private void m0001_01f() {
            //genReadIntegerSignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerSignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_020() {
            //genReadLongSignedDeltaOptional(idx, source, constAbsent, rLongDictionary)
            {
            long tmpLng=reader.readLongSignedDeltaOptional(0x1/*idx*/, 0x1/*source*/, mantDictionary, 0x7fffffffffffffffL/*constAbsent*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0001_021() {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_022() {
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, StaticGlue.INIT_VALUE_MASK | 0x6/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x1/*defLen*/);
            } else {
                StaticGlue.readASCIIToHeap(0x6/*idx*/, charDictionary, reader);
                int len = charDictionary.valueLength(0x6/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, queue.writeTextToRingBuffer(0x6/*idx*/, len));
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, len);
            }
        
    };
    private void m0001_023() {
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, StaticGlue.INIT_VALUE_MASK | 0x7/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x3/*defLen*/);
            } else {
                StaticGlue.readASCIIToHeap(0x7/*idx*/, charDictionary, reader);
                int len = charDictionary.valueLength(0x7/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, queue.writeTextToRingBuffer(0x7/*idx*/, len));
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, len);
            }
        
    };
    private void m0001_024() {
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, StaticGlue.INIT_VALUE_MASK | 0x8/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x1/*defLen*/);
            } else {
                StaticGlue.readASCIIToHeap(0x8/*idx*/, charDictionary, reader);
                int len = charDictionary.valueLength(0x8/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, queue.writeTextToRingBuffer(0x8/*idx*/, len));
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, len);
            }
        
    };
    private void m0001_025() {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_026() {
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, StaticGlue.INIT_VALUE_MASK | 0x9/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0xd/*defLen*/);
            } else {
                StaticGlue.readASCIIToHeap(0x9/*idx*/, charDictionary, reader);
                int len = charDictionary.valueLength(0x9/*idx*/);
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, queue.writeTextToRingBuffer(0x9/*idx*/, len));
                FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, len);
            }
        
    };
    private void m0001_027() {
            //genReadGroupClose()
            reader.closePMap();
        
    };
    private void m0001_028() {
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
        temp101();
        int length;
        FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, rIntDictionary[0x11/*target*/] = length = reader.readIntegerUnsigned());
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            activeScriptCursor = 0x2f/*jumpToTarget*/;
            return;// true;
        } else {
            // jumpSequence = 0;
            sequenceCountStack[++sequenceCountStackHead] = length;
           // return false;
       }
     temp100();
    }






    private void temp101() {
        assert (gatherReadData(reader, activeScriptCursor));
        m0002_029();
        m0002_02a();
        m0002_02b();
        m0002_02c();
        m0002_02d();
        m0002_02e();
    }






    private void temp100() {
        //   if (m0002_02f()) {return;};
            m0002_030();
            m0002_031();
            m0002_032();
            m0002_033();
            m0002_034();
            m0002_035();
            m0002_036();
            m0002_037();
            m0002_038();
            m0002_039();
    }
    private void m0002_029() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x8000000a/*constIdx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x5/*constLen*/);
        
    };
    private void m0002_02a() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x8000000b/*constIdx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x0/*constLen*/);
        
    };
    private void m0002_02b() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x8000000c/*constIdx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x0/*constLen*/);
        
    };
    private void m0002_02c() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, rIntDictionary[0xf/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_02d() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, rIntDictionary[0x10/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_02e() {
            //genReadASCIINone(idx)
            {
            StaticGlue.readASCIIToHeap(0xd/*idx*/, charDictionary, reader);
            int len = charDictionary.valueLength(0xd/*idx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, queue.writeTextToRingBuffer(0xd/*idx*/, len));
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, len);
            }
        
    };
    private boolean m0002_02f() {
            //genReadLength(target, jumpToTarget)
            int length;
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, rIntDictionary[0x11/*target*/] = length = reader.readIntegerUnsigned());
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
    private void m0002_030() {
            //genReadGroupPMapOpen()
            reader.openPMap(nonTemplatePMapSize);
        
    };
    private void m0002_031() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x8000000e/*constIdx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x0/*constLen*/);
        
    };
    private void m0002_032() {
            //genReadLongUnsignedOptional(constAbsent)
            {
            long tmpLng=reader.readLongUnsignedOptional(0x7fffffffffffffffL/*constAbsent*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0002_033() {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0002_034() {
            //genReadLongUnsignedNone(idx, rLongDictionary)
            {
            long tmpLng=reader.readLongUnsigned(0x1/*idx*/, rLongDictionary);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0002_035() {
            //genReadIntegerUnsignedDefault(constDefault)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, reader.readIntegerUnsignedDefault(0x1/*constDefault*/));
        
    };
    private void m0002_036() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, rIntDictionary[0x14/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_037() {
            //genReadIntegerUnsignedConstant(constDefault)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x9/*constDefault*/);
        
    };
    private void m0002_038() {
            //genReadGroupClose()
            reader.closePMap();
        
    };
    private void m0002_039() {
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
        m0099_03a();
    }
    private void m0099_03a() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x80000000/*constIdx*/);
            FASTRingBuffer.appendi(bfr, queue.addPos++, bfrMsk, 0x2/*constLen*/);
        
    };

    
}
