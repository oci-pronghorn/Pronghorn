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

    

    private void case9() { //TODO: hack this was not generated because of the new if logic????!!!!???
        case1b1();
        case1b2();
    }
    
    
    //NOTE:everything generated after this point.
    private void case1() {     // for message 1
        assert (gatherReadData(reader, activeScriptCursor));
        case1a();//TODO: this does healp
        if (m0001_012()) {return;};
        case1b();
    }



    private void case1b() {//23 is too many method
        case1b1();
        case1b2();
    }



    private void case1b2() {
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
        m0001_029();
        m0001_02a();
    }



    private void case1b1() {
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
    }



    private void case1a() {//16 looks good
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
        m0001_011();
    }
    private void m0001_001() {
            //genReadDictionaryTextReset(idx)
            charDictionary.reset(0x4/*idx*/);
        
    };
    private void m0001_002() {
            //genReadDictionaryLongReset(idx)
            rLongDictionary[0x0/*idx*/] = rLongInit[0x0/*idx*/];
        
    };
    private void m0001_003() {
            //genReadDictionaryLongReset(idx)
            rLongDictionary[0x1/*idx*/] = rLongInit[0x1/*idx*/];
        
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
            //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0xd/*idx*/] = rIntInit[0xd/*idx*/];
        
    };
    private void m0001_00b() {
            //genReadDictionaryIntegerReset(idx)
            super.rIntDictionary[0xe/*idx*/] = rIntInit[0xe/*idx*/];
        
    };
    private void m0001_00c() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x80000001/*constIdx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x3/*constLen*/);
        
    };
    private void m0001_00d() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x80000002/*constIdx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x1/*constLen*/);
        
    };
    private void m0001_00e() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x80000003/*constIdx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0xd/*constLen*/);
        
    };
    private void m0001_00f() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x0/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0001_010() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x1/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0001_011() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x2/*target*/] = reader.readIntegerUnsigned());
        
    };
    private boolean m0001_012() {
            //genReadLength(target, jumpToTarget)
            int length;
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x3/*target*/] = length = reader.readIntegerUnsigned());
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
    private void m0001_013() {
            //genReadGroupPMapOpen()
            reader.openPMap(nonTemplatePMapSize);
        
    };
    private void m0001_014() {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedCopy(0x4/*target*/, 0x4/*source*/, rIntDictionary));
        
    };
    private void m0001_015() {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_016() {
            //genReadASCIICopy(idx)
            {
            if (reader.popPMapBit()!=0) {
                StaticGlue.readASCIIToHeap(0x4/*idx*/, charDictionary, reader);
            }
            int len = charDictionary.valueLength(0x4/*idx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x4/*idx*/, len));
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_017() {
            //genReadIntegerUnsignedOptional(constAbsent)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, StaticGlue.staticReadIntegerUnsignedOptional(0x7fffffff/*constAbsent*/, reader));
        
    };
    private void m0001_018() {
            //genReadIntegerUnsignedConstant(constDefault)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x9/*constDefault*/);
        
    };
    private void m0001_019() {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedCopy(0x8/*target*/, 0x8/*source*/, rIntDictionary));
        
    };
    private void m0001_01a() {
            //genReadIntegerUnsignedIncrement(target, source, rIntDictionary)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedIncrement(0x9/*target*/, 0x9/*source*/, rIntDictionary));
        
    };
    private void m0001_01b() {
            //genReadIntegerSignedDefault(constDefault)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerSignedDefault(0x0/*constDefault*/));
        
    };
    private void m0001_01c() {
            //genReadLongSignedDelta(idx, source, rLongDictionary)
            {
            long tmpLng=reader.readLongSignedDelta(0x0/*idx*/, 0x0/*source*/, rLongDictionary);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0001_01d() {
            //genReadIntegerUnsignedCopy(target, source, rIntDictionary)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedCopy(0xb/*target*/, 0xb/*source*/, rIntDictionary));
        
    };
    private void m0001_01e() {
            //genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerSignedDeltaOptional(0xc/*target*/, 0xc/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_01f() {
            //genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDeltaOptional(0xd/*target*/, 0xd/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_020() {
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x5/*idx*/);
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x1/*defLen*/);
            } else {
                StaticGlue.readASCIIToHeap(0x5/*idx*/, charDictionary, reader);
                int len = charDictionary.valueLength(0x5/*idx*/);
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x5/*idx*/, len));
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_021() {
            //genReadIntegerSignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerSignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_022() {
            //genReadLongSignedDeltaOptional(idx, source, constAbsent, rLongDictionary)
            {
            long tmpLng=reader.readLongSignedDeltaOptional(0x1/*idx*/, 0x1/*source*/, rLongDictionary, 0x7fffffffffffffffL/*constAbsent*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0001_023() {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_024() { //TODO: sample hot spot. is this fixed by letting the short side get in lined and the long side get wrapped?
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x6/*idx*/);
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x1/*defLen*/);
            } else {
                test42(reader, charDictionary, rbRingBuffer);
            }
        
    }



    private void test42(PrimitiveReader primitiveReader, TextHeap textHeap, FASTRingBuffer ringBuffer) {
        byte val;
                int chr;
                if (0 != (chr = 0x7F & (val = primitiveReader.readTextASCIIByte()))) {// low
                                                                                      // 7
                                                                                      // bits
                                                                                      // have
                                                                                      // data
                    if (val < 0) {
                        t1(textHeap, ringBuffer, chr);
                    } else {
                        t2(primitiveReader, textHeap, ringBuffer, val);
                    }
                } else {
                    t3(primitiveReader, textHeap, ringBuffer, val);
                }

    }



    private void t3(PrimitiveReader primitiveReader, TextHeap textHeap, FASTRingBuffer ringBuffer, byte val) {
        testOther(primitiveReader, textHeap, val);
        int len = textHeap.valueLength(0x6/*idx*/);
        FASTRingBuffer.appendi(rbBuffer, ringBuffer.addPos++, rbMask, ringBuffer.writeTextToRingBuffer(0x6/*idx*/, len));
        FASTRingBuffer.appendi(rbBuffer, ringBuffer.addPos++, rbMask, len);
    }



    private void t2(PrimitiveReader primitiveReader, TextHeap textHeap, FASTRingBuffer ringBuffer, byte val) {
        StaticGlue.readASCIIToHeapValueLong(val, 0x6, textHeap, primitiveReader);
        int len = textHeap.valueLength(0x6/*idx*/);
        FASTRingBuffer.appendi(rbBuffer, ringBuffer.addPos++, rbMask, ringBuffer.writeTextToRingBuffer(0x6/*idx*/, len));
        FASTRingBuffer.appendi(rbBuffer, ringBuffer.addPos++, rbMask, len);
    }



    private void t1(TextHeap textHeap, FASTRingBuffer ringBuffer, int chr) {
        textHeap.setSingleCharText((char) chr, 0x6);//XX: called all the time???
        FASTRingBuffer.appendi(rbBuffer, ringBuffer.addPos++, rbMask, ringBuffer.writeTextToRingBuffer(0x6/*idx*/, 1));
        FASTRingBuffer.appendi(rbBuffer, ringBuffer.addPos++, rbMask, 1);
    }



    private void testOther(PrimitiveReader primitiveReader, TextHeap textHeap, byte val) {
        // 0x80 is a null string.
        // 0x00, 0x80 is zero length string
        if (0 == val) {
            // almost never happens
            textHeap.setZeroLength(0x6);
            // must move cursor off the second byte
            val = primitiveReader.readTextASCIIByte(); // < .1%
            // at least do a validation because we already have what we need
            assert ((val & 0xFF) == 0x80);
        } else {
            // happens rarely when it equals 0x80
            textHeap.setNull(0x6);
        
        }
    };
    private void m0001_025() {
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x7/*idx*/);
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x3/*defLen*/);
            } else {
                StaticGlue.readASCIIToHeap(0x7/*idx*/, charDictionary, reader);
                int len = charDictionary.valueLength(0x7/*idx*/);
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x7/*idx*/, len));
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_026() {
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x8/*idx*/);
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x1/*defLen*/);
            } else {
                StaticGlue.readASCIIToHeap(0x8/*idx*/, charDictionary, reader);
                int len = charDictionary.valueLength(0x8/*idx*/);
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x8/*idx*/, len));
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_027() {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0001_028() {
            //genReadASCIIDefault(idx, defLen)
            if (0 == reader.popPMapBit()) {
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, StaticGlue.INIT_VALUE_MASK | 0x9/*idx*/);
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0xd/*defLen*/);
            } else {
                StaticGlue.readASCIIToHeap(0x9/*idx*/, charDictionary, reader);
                int len = charDictionary.valueLength(0x9/*idx*/);
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0x9/*idx*/, len));
                FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private void m0001_029() {
            //genReadGroupClose()
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


    private void case32() {     // for message 2
        assert (gatherReadData(reader, activeScriptCursor));
        m0002_02b();
        m0002_02c();
        m0002_02d();
        m0002_02e();
        m0002_02f();
        m0002_030();
        if (m0002_031()) {return;};
        m0002_032();
        m0002_033();
        m0002_034();
        m0002_035();
        m0002_036();
        m0002_037();
        m0002_038();
        m0002_039();
        m0002_03a();
        m0002_03b();
    }
    private void m0002_02b() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x8000000a/*constIdx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x5/*constLen*/);
        
    };
    private void m0002_02c() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x8000000b/*constIdx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x0/*constLen*/);
        
    };
    private void m0002_02d() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x8000000c/*constIdx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x0/*constLen*/);
        
    };
    private void m0002_02e() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x11/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_02f() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x12/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_030() {
            //genReadASCIINone(idx)
            {
            StaticGlue.readASCIIToHeap(0xd/*idx*/, charDictionary, reader);
            int len = charDictionary.valueLength(0xd/*idx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rbRingBuffer.writeTextToRingBuffer(0xd/*idx*/, len));
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, len);
            }
        
    };
    private boolean m0002_031() {
            //genReadLength(target, jumpToTarget)
            int length;
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x13/*target*/] = length = reader.readIntegerUnsigned());
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
    private void m0002_032() {
            //genReadGroupPMapOpen()
            reader.openPMap(nonTemplatePMapSize);
        
    };
    private void m0002_033() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x8000000e/*constIdx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x0/*constLen*/);
        
    };
    private void m0002_034() {
            //genReadLongUnsignedOptional(constAbsent)
            {
            long tmpLng=reader.readLongUnsignedOptional(0x7fffffffffffffffL/*constAbsent*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0002_035() {
            //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    };
    private void m0002_036() {
            //genReadLongUnsignedNone(idx, rLongDictionary)
            {
            long tmpLng=reader.readLongUnsigned(0x3/*idx*/, rLongDictionary);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    };
    private void m0002_037() {
            //genReadIntegerUnsignedDefault(constDefault)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, reader.readIntegerUnsignedDefault(0x1/*constDefault*/));
        
    };
    private void m0002_038() {
            //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, rIntDictionary[0x16/*target*/] = reader.readIntegerUnsigned());
        
    };
    private void m0002_039() {
            //genReadIntegerUnsignedConstant(constDefault)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x9/*constDefault*/);
        
    };
    private void m0002_03a() {
            //genReadGroupClose()
            reader.closePMap();
        
    };
    private void m0002_03b() {
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
        assert (super.gatherReadData(reader, activeScriptCursor));
        m0099_03c();
    }
    private void m0099_03c() {
            //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x80000000/*constIdx*/);
            FASTRingBuffer.appendi(rbBuffer, rbRingBuffer.addPos++, rbMask, 0x2/*constLen*/);
        
    };
}
