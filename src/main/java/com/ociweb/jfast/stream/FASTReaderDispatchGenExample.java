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
//        int c = activeScriptCursor;
//        if (0 == (c & 1)) {
//            //0 or 32
//            if (0==c) {
//                return top0();
//            } else {
//                return top32();
//            }
//            
//        } else {
//            //1 or 9
//            if (1==c) {
//                return top1();
//            } else {
//                return top9();
//            }
//            
//            
//        }
        
        
        switch (activeScriptCursor) {
            case 0:
            return top0();
            case 1:
            return top1();
            case 9:     
            return top9();

            case 32://30:
            return top32();
            default:
                assert (false) : "Unsupported Template";
                return false;
        }
    }

    private boolean top0() {
        assert (gatherReadData(reader, activeScriptCursor));
        case0();
        activeScriptCursor = 1; //jumping to end of 0
        return false;
    }

    private boolean top1() {
        assert (gatherReadData(reader, activeScriptCursor));
        case1();
        int length = rIntDictionary[0x3/*target*/];
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            activeScriptCursor = 31; //Jumping to END of 1?
            return false;
        }
        sequenceCountStack[++sequenceCountStackHead] = length;
        return top9();
    }

    private boolean top9() {
        assert (gatherReadData(reader, 9));
        
        case9(); //mantDictionary inserted in place of long dictionary
        
        //TODO: generated code will not have this?
        checkSequence = (0xc0dc0016 & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)); //14
        activeScriptCursor = 29+2; // +1 +1 //jumping to end of 1
        assert (gatherReadData(reader, activeScriptCursor));
        return checkSequence != 0 && completeSequence(0x16);//0x014);
    }

    private boolean top32() {
        assert (gatherReadData(reader, activeScriptCursor));
        case32();
        
        int length2 = rIntDictionary[0x11/*target*/];//queue.appendInt1(reader.readIntegerUnsigned());// readIntegerUnsigned(0xd00c0011));
        if (length2 == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            activeScriptCursor = 46+2;// 36+10;  +2
            return false;
        } else {
            sequenceCountStack[++sequenceCountStackHead] = length2;
        }
        
       //   case39();
         
        reader.openPMap(nonTemplatePMapSize);
        queue.appendInt2(0x8000000e, 0x0); // ASCIIConstant(0xa02c000e
        case30b();
        closeGroup(0xc0dc0008);
        
        
        
        
        activeScriptCursor = 45+2;  // +2
        return checkSequence != 0 && completeSequence(0x008);
    }
    
    private void case0() {
        int p = queue.addPos;
    //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x80000000/*constIdx*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x2/*constLen*/);
        
        queue.addPos=p;
    }

    private void case1() {
        int p = queue.addPos;
    //genReadDictionaryDecimalReset(idx)
            expDictionary[0x0/*idx*/] = expInit[0x0/*idx*/];
            mantDictionary[0x0/*idx*/] = mantInit[0x0/*idx*/];
        
    //genReadDictionaryDecimalReset(idx)
            expDictionary[0x1/*idx*/] = expInit[0x1/*idx*/];
            mantDictionary[0x1/*idx*/] = mantInit[0x1/*idx*/];
        
    //genReadDictionaryTextReset(idx)
            charDictionary.reset(0x4/*idx*/);
        
    //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0x4/*idx*/] = rIntInit[0x4/*idx*/];
        
    //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0x8/*idx*/] = rIntInit[0x8/*idx*/];
        
    //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0x9/*idx*/] = rIntInit[0x9/*idx*/];
        
    //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0xa/*idx*/] = rIntInit[0xa/*idx*/];
        
    //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0xb/*idx*/] = rIntInit[0xb/*idx*/];
        
    //genReadDictionaryIntegerReset(idx)
            rIntDictionary[0xc/*idx*/] = rIntInit[0xc/*idx*/];
        
    //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x80000001/*constIdx*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x3/*constLen*/);
        
    //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x80000002/*constIdx*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x1/*constLen*/);
        
    //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x80000003/*constIdx*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0xd/*constLen*/);
        
    //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, rIntDictionary[0x0/*target*/] = reader.readIntegerUnsigned());
        
    //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, rIntDictionary[0x1/*target*/] = reader.readIntegerUnsigned());
        
    //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, rIntDictionary[0x2/*target*/] = reader.readIntegerUnsigned());
        
    //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, rIntDictionary[0x3/*target*/] = reader.readIntegerUnsigned());
        
        queue.addPos=p;
    }
    

    //TODO: what if generator adds private methods for every group field!!
    
    private void case9() {
        int p = queue.addPos;
        int[] bfr2 = bfr;
        int bfrMsk2 = bfrMsk;
        PrimitiveReader reader2 = reader; //this change did not help
        
        
        
        
    test1(reader2);
        
    p = test2(p, bfr2, bfrMsk2, reader2);
        
    p = test3(p, bfr2, bfrMsk2, reader2);
        
    p = test4(p, bfr2, bfrMsk2, reader2);
        
    p = test5(p, bfr2, bfrMsk2, reader2);
        
    p = test6(p, bfr2, bfrMsk2);
        
    p = test7(p, bfr2, bfrMsk2, reader2);
        
    p = test8(p, bfr2, bfrMsk2, reader2);
        
    p = test9(p, bfr2, bfrMsk2, reader2);
        
    p = test10(p, bfr2, bfrMsk2, reader2);
        
    p = test11(p, bfr2, bfrMsk2, reader2);
        
    p = test12(p, bfr2, bfrMsk2, reader2);
        
    p = test13(p, bfr2, bfrMsk2, reader2);
        
    p = test14(p, bfr2, bfrMsk2, reader2);
        
    p = test15(p, bfr2, bfrMsk2, reader2);
        
    p = test16(p, bfr2, bfrMsk2, reader2);
        
    p = test17(p, bfr2, bfrMsk2, reader2);
        
    p = test21(p, bfr2, bfrMsk2, reader2);
        
    p = test22(p, bfr2, bfrMsk2, reader2);
        
    p = test23(p, bfr2, bfrMsk2, reader2);
        
    p = test17(p, bfr2, bfrMsk2, reader2);
        
    p = test24(p, bfr2, bfrMsk2, reader2);
        
    //genReadGroupClose()
            reader2.closePMap();
        
        queue.addPos=p;
    }
    
    //each field as a method helps the most
    //Duration:13030981ns  2.3022826MM/s  6.157738nspB  1299mbps  In:2116196 Out:113510784 pct 0.018643128 Messages:30001 Groups:90001
    //Duration:12868159ns  2.3314135MM/s  6.080797nspB  1315mbps  In:2116196 Out:113510784 pct 0.018643128 Messages:30001 Groups:90001
    //Duration:12813111ns  2.3414297MM/s  6.054785nspB  1321mbps  In:2116196 Out:113510784 pct 0.018643128 Messages:30001 Groups:90001
    //Duration:12765445ns  2.3501728MM/s  6.0322604nspB  1326mbps  In:2116196 Out:113510784 pct 0.018643128 Messages:30001 Groups:90001

    private int test24(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadASCIIDefault(idx, defLen)
        if (0 == reader2.popPMapBit()) {
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, StaticGlue.INIT_VALUE_MASK | 0x9/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, 0xd/*defLen*/);
        } else {
            StaticGlue.readASCIIToHeap(0x9/*idx*/, charDictionary, reader2);
            int len = charDictionary.valueLength(0x9/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, queue.writeTextToRingBuffer(0x9/*idx*/, len));
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, len);
        }
        return p;
    }

    private int test23(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadASCIIDefault(idx, defLen)
        if (0 == reader2.popPMapBit()) {
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, StaticGlue.INIT_VALUE_MASK | 0x8/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, 0x1/*defLen*/);
        } else {
            StaticGlue.readASCIIToHeap(0x8/*idx*/, charDictionary, reader2);
            int len = charDictionary.valueLength(0x8/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, queue.writeTextToRingBuffer(0x8/*idx*/, len));
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, len);
        }
        return p;
    }

    private int test22(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadASCIIDefault(idx, defLen)
        if (0 == reader2.popPMapBit()) {
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, StaticGlue.INIT_VALUE_MASK | 0x7/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, 0x3/*defLen*/);
        } else {
            StaticGlue.readASCIIToHeap(0x7/*idx*/, charDictionary, reader2);
            int len = charDictionary.valueLength(0x7/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, queue.writeTextToRingBuffer(0x7/*idx*/, len));
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, len);
        }
        return p;
    }

    private int test21(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadASCIIDefault(idx, defLen)
        if (0 == reader2.popPMapBit()) {
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, StaticGlue.INIT_VALUE_MASK | 0x6/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, 0x1/*defLen*/);
        } else {
            StaticGlue.readASCIIToHeap(0x6/*idx*/, charDictionary, reader2);
            int len = charDictionary.valueLength(0x6/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, queue.writeTextToRingBuffer(0x6/*idx*/, len));
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, len);
        }
        return p;
    }

    private int test17(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerUnsignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        return p;
    }

    private int test16(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadLongSignedDeltaOptional(idx, source, constAbsent, rLongDictionary)
        {
        long tmpLng=reader2.readLongSignedDeltaOptional(0x1/*idx*/, 0x1/*source*/, mantDictionary, 0x7fffffffffffffffL/*constAbsent*/);
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, (int) (tmpLng & 0xFFFFFFFF));
        }
        return p;
    }

    private int test15(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerSignedDefaultOptional(constAbsent, constDefault)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerSignedDefaultOptional(0x7fffffff/*constDefault*/, 0x7fffffff/*constAbsent*/));
        return p;
    }

    private int test14(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadASCIIDefault(idx, defLen)
        if (0 == reader2.popPMapBit()) {
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, StaticGlue.INIT_VALUE_MASK | 0x5/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, 0x1/*defLen*/);
        } else {
            StaticGlue.readASCIIToHeap(0x5/*idx*/, charDictionary, reader2);
            int len = charDictionary.valueLength(0x5/*idx*/);
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, queue.writeTextToRingBuffer(0x5/*idx*/, len));
            FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, len);
        }
        return p;
    }

    private int test13(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerUnsignedDeltaOptional(0xc/*target*/, 0xc/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        return p;
    }

    private int test12(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerSignedDeltaOptional(0xb/*target*/, 0xb/*source*/, rIntDictionary, 0x7fffffff/*constAbsent*/));
        return p;
    }

    private int test11(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerUnsignedCopy(target, source, rIntDictionary)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerUnsignedCopy(0xa/*target*/, 0xa/*source*/, rIntDictionary));
        return p;
    }

    private int test10(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadLongSignedDelta(idx, source, rLongDictionary)
        {
        long tmpLng=reader2.readLongSignedDelta(0x0/*idx*/, 0x0/*source*/, mantDictionary);
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, (int) (tmpLng & 0xFFFFFFFF));
        }
        return p;
    }

    private int test9(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerSignedDefault(constDefault)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerSignedDefault(0x0/*constDefault*/));
        return p;
    }

    private int test8(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerUnsignedIncrement(target, source, rIntDictionary)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerUnsignedIncrement(0x9/*target*/, 0x9/*source*/, rIntDictionary));
        return p;
    }

    private int test7(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerUnsignedCopy(target, source, rIntDictionary)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerUnsignedCopy(0x8/*target*/, 0x8/*source*/, rIntDictionary));
        return p;
    }

    private int test6(int p, int[] bfr2, int bfrMsk2) {
        //genReadIntegerUnsignedConstant(constDefault)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, 0x9/*constDefault*/);
        return p;
    }

    private int test5(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerUnsignedOptional(constAbsent)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, StaticGlue.staticReadIntegerUnsignedOptional(0x7fffffff/*constAbsent*/, reader2));
        return p;
    }

    private int test4(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadASCIICopy(idx)
        {
        if (reader2.popPMapBit()!=0) {
            StaticGlue.readASCIIToHeap(0x4/*idx*/, charDictionary, reader2);
        }
        int len = charDictionary.valueLength(0x4/*idx*/);
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, queue.writeTextToRingBuffer(0x4/*idx*/, len));
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, len);
        }
        return p;
    }

    private int test3(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        return p;
    }

    private int test2(int p, int[] bfr2, int bfrMsk2, PrimitiveReader reader2) {
        //genReadIntegerUnsignedCopy(target, source, rIntDictionary)
        FASTRingBuffer.appendi(bfr2, p++, bfrMsk2, reader2.readIntegerUnsignedCopy(0x4/*target*/, 0x4/*source*/, rIntDictionary));
        return p;
    }

    private void test1(PrimitiveReader reader2) {
        //genReadGroupPMapOpen()
        reader2.openPMap(nonTemplatePMapSize);
    }


    private void case32() {
        int p = queue.addPos;
    //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x8000000a/*constIdx*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x5/*constLen*/);
        
    //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x8000000b/*constIdx*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x0/*constLen*/);
        
    //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x8000000c/*constIdx*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x0/*constLen*/);
        
    //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, rIntDictionary[0xf/*target*/] = reader.readIntegerUnsigned());
        
    //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, rIntDictionary[0x10/*target*/] = reader.readIntegerUnsigned());
        
    //genReadASCIINone(idx)
            {
            StaticGlue.readASCIIToHeap(0xd/*idx*/, charDictionary, reader);
            int len = charDictionary.valueLength(0xd/*idx*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, queue.writeTextToRingBuffer(0xd/*idx*/, len));
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, len);
            }
        
    //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, rIntDictionary[0x11/*target*/] = reader.readIntegerUnsigned());
        
        queue.addPos=p;
    }

    private void case30b() {
        long xl1;
        queue.appendInt8((int) ((xl1 = reader.readLongUnsignedOptional(constLongAbsent)) >>> 32),
                (int) (xl1 & 0xFFFFFFFF), reader.readIntegerUnsignedDefaultOptional(1/*
                                                                                      * default
                                                                                      * or
                                                                                      * optional
                                                                                      */, constIntAbsent),
                (int) ((xl1 = reader.readLongUnsigned(0x01, rLongDictionary)) >>> 32), (int) (xl1 & 0xFFFFFFFF),
                reader.readIntegerUnsignedDefault(1/* default value */), reader.readIntegerUnsigned(),
                rIntDictionary[0x15]);
    }

    private void case39() {
        int p = queue.addPos;
    //genReadGroupPMapOpen()
            reader.openPMap(nonTemplatePMapSize);
        
    //genReadTextConstant(constIdx, constLen)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x8000000e/*constIdx*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x0/*constLen*/);
        
    //genReadLongUnsignedOptional(constAbsent)
            {
            long tmpLng=reader.readLongUnsignedOptional(0x7fffffffffffffffL/*constAbsent*/);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    //genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, reader.readIntegerUnsignedDefaultOptional(0x1/*constDefault*/, 0x7fffffff/*constAbsent*/));
        
    //genReadLongUnsignedNone(idx, rLongDictionary)
            {
            long tmpLng=reader.readLongUnsigned(0x1/*idx*/, rLongDictionary);
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, (int) (tmpLng & 0xFFFFFFFF));
            }
        
    //genReadIntegerUnsignedDefault(constDefault)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, reader.readIntegerUnsignedDefault(0x1/*constDefault*/));
        
    //genReadIntegerUnsigned(target)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, rIntDictionary[0x14/*target*/] = reader.readIntegerUnsigned());
        
    //genReadIntegerUnsignedConstant(constDefault)
            FASTRingBuffer.appendi(bfr, p++, bfrMsk, 0x9/*constDefault*/);
        
    //genReadGroupClose()
            reader.closePMap();
        
        queue.addPos=p;
    }
    

    
}
