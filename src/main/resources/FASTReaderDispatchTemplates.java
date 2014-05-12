package com.ociweb.jfast.generator;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.StaticGlue;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTReaderDispatchBase;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;

public abstract class FASTReaderDispatchTemplates extends FASTReaderDispatchBase {


    public FASTReaderDispatchTemplates(TemplateCatalog catalog) {
        super(catalog);
    }
    
    public FASTReaderDispatchTemplates(byte[] catBytes) {
        super(new TemplateCatalog(catBytes));
    }
    
    //second constructor only needed for testing.
    protected FASTReaderDispatchTemplates(DictionaryFactory dcr, int nonTemplatePMapSize, int[][] dictionaryMembers,
            int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript, int maxNestedGroupDepth,
            int primaryRingBits, int textRingBits) {
        super(dcr, nonTemplatePMapSize, dictionaryMembers, maxTextLen, maxVectorLen, charGap, bytesGap, fullScript, maxNestedGroupDepth,
                primaryRingBits, textRingBits);
    }

    
    protected void genReadCopyText(int source, int target, TextHeap textHeap) {
        textHeap.copy(source,target);
    }

    protected void genReadCopyBytes(int source, int target, ByteHeap byteHeap) {
        byteHeap.copy(source,target);
    }
    
    protected void genReadSequenceClose(int backvalue, FASTReaderDispatchBase dispatch) {
        if (dispatch.sequenceCountStackHead <= 0) {
            // no sequence to worry about or not the right time
            dispatch.doSequence = false;
        } else {
        
            // each sequence will need to repeat the pmap but we only need to push
            // and pop the stack when the sequence is first encountered.
            // if count is zero we can pop it off but not until then.
        
            if (--dispatch.sequenceCountStack[dispatch.sequenceCountStackHead] < 1) {
                // this group is a sequence so pop it off the stack.
                --dispatch.sequenceCountStackHead;
                // finished this sequence so leave pointer where it is
                dispatch.jumpSequence = 0;
            } else {
                // do this sequence again so move pointer back
                dispatch.jumpSequence = backvalue;
            }
            dispatch.doSequence = true;
        }
    }
    
    protected void genReadGroupPMapOpen(int nonTemplatePMapSize, PrimitiveReader reader) {
        PrimitiveReader.openPMap(nonTemplatePMapSize, reader);
    }

    protected void genReadGroupClose(PrimitiveReader reader) {
        PrimitiveReader.closePMap(reader);
    }
    
    
    //length methods
    protected boolean genReadLengthDefault(int constDefault,  int jumpToTarget, int[] rbB, PrimitiveReader reader, int rbMask, FASTRingBuffer rbRingBuffer, FASTReaderDispatchBase dispatch) {
        
        int length;
        int value = length = PrimitiveReader.readIntegerUnsignedDefault(constDefault, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            dispatch.activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
            return false;
       }
    }

    //TODO: C, once this all works find a better way to inline it with only 1 conditional.
    protected boolean genReadLengthIncrement(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTReaderDispatchBase dispatch) {
        int length;
        int value = length = PrimitiveReader.readIntegerUnsignedIncrement(target, source, rIntDictionary, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            dispatch.activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
            return false;
       }
    }

    protected boolean genReadLengthCopy(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTReaderDispatchBase dispatch) {
        int length;
        int value = length = PrimitiveReader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            dispatch.activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
            return false;
       }
    }

    protected boolean genReadLengthConstant(int constDefault,  int jumpToTarget, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, FASTReaderDispatchBase dispatch) {
        rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
        if (constDefault == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            dispatch.activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = constDefault;
            return false;
       }
    }

    protected boolean genReadLengthDelta(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTReaderDispatchBase dispatch) {
        int length;
        int value = length = (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader)));
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            dispatch.activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
            return false;
       }
    }

    protected boolean genReadLength(int target,  int jumpToTarget, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, int[] rIntDictionary, PrimitiveReader reader, FASTReaderDispatchBase dispatch) {
        int length;
        int value = rIntDictionary[target] = length = PrimitiveReader.readIntegerUnsigned(reader);
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            dispatch.activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
            return false;
       }
    }
    
    //TODO: A, needs support for messageRef where we can inject template in another and return to the previouslocation. Needs STACK in dispatch!
    //TODO: Z, can we send catalog in-band as a byteArray to push dynamic changes,  Need a unit test for this.
    //TODO: B, Modify code generator to share called function whenever possible, faster inline and smaller footprint
    //TODO: B, set the default template for the case when it is undefined in catalog.
    //TODO: C, Must add unit test for message length field start-of-frame testing, FrameLength bytes to read before decoding, is before pmap/templateId
    //TODO: D, perhaps frame support is related to buffer size in primtive write so the right number of bits can be set.
    //TODO: X, Add undecoded field option so caller can deal with the subtraction of optinals.
    
    // int methods

    protected void genReadIntegerUnsignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerUnsignedDefaultOptional(constDefault, constAbsent, reader);
    }

    protected void genReadIntegerUnsignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerUnsignedIncrementOptional(target, source, rIntDictionary, constAbsent, reader);
    }

    protected void genReadIntegerUnsignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int xi1;
        rbB[rbMask & rbRingBuffer.addPos++] = (0 == (xi1 = PrimitiveReader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader)) ? constAbsent : xi1 - 1);
    }

    protected void genReadIntegerUnsignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst);
    }

    protected void genReadIntegerUnsignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        // Delta opp never uses PMAP
        long value = PrimitiveReader.readLongSigned(reader);
        int result;
        if (0 == value) {
            rIntDictionary[target] = 0;// set to absent
            result = constAbsent;
        } else {
            result = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
        
        }
        rbB[rbMask & rbRingBuffer.addPos++] = result;
    }

    protected void genReadIntegerUnsignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int xi1;
        rbB[rbMask & rbRingBuffer.addPos++] = 0 == (xi1 = PrimitiveReader.readIntegerUnsigned(reader)) ? constAbsent : xi1 - 1;
    }

    protected void genReadIntegerUnsignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerUnsignedDefault(constDefault, reader);
    }

    protected void genReadIntegerUnsignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerUnsignedIncrement(target, source, rIntDictionary, reader);
    }

    protected void genReadIntegerUnsignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader);
    }

    protected void genReadIntegerUnsignedConstant(int constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
    }

    protected void genReadIntegerUnsignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader)));
    }

    protected void genReadIntegerUnsigned(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = PrimitiveReader.readIntegerUnsigned(reader);
    }

    protected void genReadIntegerSignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerSignedDefault(constDefault, reader);
    }

    protected void genReadIntegerSignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerSignedIncrement(target, source, rIntDictionary, reader);
    }

    protected void genReadIntegerSignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader);
    }

    protected void genReadIntegerConstant(int constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
    }

    protected void genReadIntegerSignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSignedPrivate(reader)));
    }

    protected void genReadIntegerSignedNone(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = PrimitiveReader.readIntegerSigned(reader);
    }

    protected void genReadIntegerSignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerSignedDefaultOptional(constDefault, constAbsent, reader);
    }

    protected void genReadIntegerSignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerSignedIncrementOptional(target, source, rIntDictionary, constAbsent, reader);
    }

    protected void genReadIntegerSignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int xi1;
        rbB[rbMask & rbRingBuffer.addPos++] = (0 == (xi1 = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader)) ? constAbsent : (xi1 > 0 ? xi1 - 1 : xi1));
    }

    protected void genReadIntegerSignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst);
    }

    protected void genReadIntegerSignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        // Delta opp never uses PMAP
        long value = PrimitiveReader.readLongSignedPrivate(reader);
        int result;
        if (0 == value) {
            rIntDictionary[target] = 0;// set to absent
            result = constAbsent;
        } else {
            result = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
        }
        rbB[rbMask & rbRingBuffer.addPos++] = result;
    }

    protected void genReadIntegerSignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int value = PrimitiveReader.readIntegerSigned(reader);
        rbB[rbMask & rbRingBuffer.addPos++] = value == 0 ? constAbsent : (value > 0 ? value - 1 : value);
    }

    // long methods

    protected void genReadLongUnsignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongUnsignedDefault(constDefault, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedIncrement(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongUnsignedIncrement(idx, source, rLongDictionary, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedCopy(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongUnsignedCopy(idx, source, rLongDictionary, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongConstant(long constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        long tmpLng=constDefault;
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedDelta(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=(rLongDictionary[idx] = (rLongDictionary[source] + PrimitiveReader.readLongSigned(reader)));
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedNone(int idx, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=rLongDictionary[idx] = PrimitiveReader.readLongUnsigned(reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongUnsignedDefaultOptional(constDefault, constAbsent, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedIncrementOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongUnsignedIncrementOptional(idx, source, rLongDictionary, constAbsent, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedCopyOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long xl1;
        long tmpLng=(0 == (xl1 = PrimitiveReader.readLongUnsignedCopy(idx, source, rLongDictionary, reader)) ? constAbsent : xl1 - 1);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
            //TODO: X, rewrite so long values are pre shifted and masked for these constants.
        long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedDeltaOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        // Delta opp never uses PMAP
        long value = PrimitiveReader.readLongSignedPrivate(reader);
        long tmpLng;
        if (0 == value) {
            rLongDictionary[idx] = 0;// set to absent
            tmpLng = constAbsent;
        } else {
            tmpLng = rLongDictionary[idx] = (rLongDictionary[source] + (value > 0 ? value - 1 : value));
        }
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long value = PrimitiveReader.readLongUnsigned(reader);
        long tmpLng=value == 0 ? constAbsent : value - 1;
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongSignedDefault(constDefault, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedIncrement(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongSignedIncrement(idx, source, rLongDictionary, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }
 //TODO: C, test if using ringBuffer reference instead of rbMask is better?
    protected void genReadLongSignedCopy(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongSignedCopy(idx, source, rLongDictionary, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedConstant(long constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        long tmpLng=constDefault;
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDelta(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=(rLongDictionary[idx] = (rLongDictionary[source] + PrimitiveReader.readLongSigned(reader)));
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedNone(int idx, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=rLongDictionary[idx] = PrimitiveReader.readLongSigned(reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongSignedDefaultOptional(constDefault, constAbsent, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedIncrementOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) { //TODO: B, Document, anything at the end is ignored and can be injected values.
        long tmpLng=PrimitiveReader.readLongSignedIncrementOptional(idx, source, rLongDictionary, constAbsent, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedCopyOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long xl1;
        long tmpLng=(0 == (xl1 = PrimitiveReader.readLongSignedCopy(idx, source, rLongDictionary, reader)) ? constAbsent : xl1 - 1);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
            //TODO: B, Performance of reader,  if long const and default values are sent as int tuples then the shift mask logic can be skipped!!!
        long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDeltaOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        // Delta opp never uses PMAP
        long value = PrimitiveReader.readLongSignedPrivate(reader);
        int a = rbMask & rbRingBuffer.addPos++;
        int b = rbMask & rbRingBuffer.addPos++;
        if (0 == value) {
            rLongDictionary[idx] = 0;// set to absent
            rbB[a] = (int) (constAbsent >>> 32); 
            rbB[b] = (int) (constAbsent & 0xFFFFFFFF);
        } else {
            long tmpLng = rLongDictionary[idx] = (rLongDictionary[source] + (value > 0 ? value - 1 : value));
            rbB[a] = (int) (tmpLng >>> 32); 
            rbB[b] = (int) (tmpLng & 0xFFFFFFFF);
        }
    }

    protected void genReadLongSignedNoneOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long value = PrimitiveReader.readLongSigned(reader);
        long tmpLng=value == 0 ? constAbsent : (value > 0 ? value - 1 : value);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    // text methods.

    
    protected void genReadUTF8None(int idx, int optOff, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader) - optOff);
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8TailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim == 0) {
            textHeap.setNull(idx);
        } else {
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
            int t = trim - 1;
            StaticGlue.allocateAndAppendUTF8(idx, textHeap, reader, utfLength, t);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8DeltaOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
        if (0 == trim) {
            textHeap.setNull(idx);
        } else {
            StaticGlue.allocateAndDeltaUTF8(idx, textHeap, reader, trim>0?trim-1:trim);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadUTF8Delta(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        StaticGlue.allocateAndDeltaUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerSigned(reader));
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadASCIITail(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        StaticGlue.readASCIITail(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader));
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadTextConstant(int constIdx, int constLen, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constIdx;
        rbB[rbMask & rbRingBuffer.addPos++] = constLen;
    }

    protected void genReadASCIIDelta(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
        if (trim >=0) {
            StaticGlue.readASCIITail(idx, textHeap, reader, trim); 
        } else {
            StaticGlue.readASCIIHead(idx, trim, textHeap, reader);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadASCIICopy(int idx, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
            int len = (PrimitiveReader.popPMapBit(reader)!=0) ? StaticGlue.readASCIIToHeap(idx, reader, textHeap) : textHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadASCIICopyOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
            int len = (PrimitiveReader.popPMapBit(reader) != 0) ? StaticGlue.readASCIIToHeap(idx, reader, textHeap) : textHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8Tail(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
            int trim = PrimitiveReader.readIntegerSigned(reader);
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
            
            StaticGlue.allocateAndAppendUTF8(idx, textHeap, reader, utfLength, trim);
            int len = textHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
    }


    protected void genReadUTF8Copy(int idx, int optOff, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) != 0) {
            StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader) - optOff);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8Default(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 == PrimitiveReader.popPMapBit(reader)) {
            rbB[rbMask & rbRingBuffer.addPos++] = defIdx;
            rbB[rbMask & rbRingBuffer.addPos++] = defLen;
        } else {
            StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader) - optOff);
            int len = textHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
        }
    }

    protected void genReadASCIINone(int idx, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        byte val;
        int tmp;
        if (0 != (tmp = 0x7F & (val = PrimitiveReader.readTextASCIIByte(reader)))) {
            tmp=StaticGlue.readASCIIToHeapValue(idx, val, tmp, textHeap, reader);
        } else {
            tmp=StaticGlue.readASCIIToHeapNone(idx, val, textHeap, reader);
        }
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, tmp, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = tmp;
    }

    protected void genReadASCIITailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int tail = PrimitiveReader.readIntegerUnsigned(reader);
        if (0 == tail) {
            textHeap.setNull(idx);
        } else {
           StaticGlue.readASCIITail(idx, textHeap, reader, tail-1);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadASCIIDeltaOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        //TODO: B, extract the constant length from here.
        int optionalTrim = PrimitiveReader.readIntegerSigned(reader);
        int tempId = (0 == optionalTrim ? 
                         textHeap.initStartOffset( FASTReaderInterpreterDispatch.INIT_VALUE_MASK | idx) |FASTReaderInterpreterDispatch.INIT_VALUE_MASK : 
                         (optionalTrim > 0 ? StaticGlue.readASCIITail(idx, textHeap, reader, optionalTrim - 1) :
                                             StaticGlue.readASCIIHead(idx, optionalTrim, textHeap, reader)));
        int len = textHeap.valueLength(tempId);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(tempId, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadTextConstantOptional(int constInit, int constValue, int constInitLen, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 != PrimitiveReader.popPMapBit(reader) ) {
            rbB[rbMask & rbRingBuffer.addPos++] = constInit;
            rbB[rbMask & rbRingBuffer.addPos++] = constInitLen;
        } else {
            rbB[rbMask & rbRingBuffer.addPos++] = constValue;
            rbB[rbMask & rbRingBuffer.addPos++] = constValueLen;
        }
    }

    protected void genReadASCIIDefault(int idx, int defIdx, int defLen, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
            int a = rbMask & rbRingBuffer.addPos++;
            int b = rbMask & rbRingBuffer.addPos++;
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                rbB[a] = defIdx;
                rbB[b] = defLen;
            } else {
                int len = StaticGlue.readASCIIToHeap(idx, reader, textHeap);
                rbB[a] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
                rbB[b] = len;
            } 
    }

        
    //byte methods
    
    protected void genReadBytesConstant(int constIdx, int constLen, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constIdx;
        rbB[rbMask & rbRingBuffer.addPos++] = constLen;
    }

    protected void genReadBytesConstantOptional(int constInit, int constInitLen, int constValue, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        
        if (0 != PrimitiveReader.popPMapBit(reader) ) {
            rbB[rbMask & rbRingBuffer.addPos++] = constInit;
            rbB[rbMask & rbRingBuffer.addPos++] = constInitLen;
        } else {
            rbB[rbMask & rbRingBuffer.addPos++] = constValue;
            rbB[rbMask & rbRingBuffer.addPos++] = constValueLen;
        }
    }

    protected void genReadBytesDefault(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        
        if (0 == PrimitiveReader.popPMapBit(reader)) {
            rbB[rbMask & rbRingBuffer.addPos++] = defIdx;
            rbB[rbMask & rbRingBuffer.addPos++] = defLen;
        } else {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - optOff;
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
            int len = byteHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
        }
    }

    protected void genReadBytesCopy(int idx, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) != 0) {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - optOff;
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
        }
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadBytesDeltaOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
        if (0 == trim) {
            byteHeap.setNull(idx);
        } else {
            if (trim > 0) {
                trim--;// subtract for optional
            }
        
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
        
            if (trim >= 0) {
                // append to tail
                PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength, reader);
            } else {
                // append to head
                PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength, reader);
            }
        }
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadBytesTailOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim == 0) {
            byteHeap.setNull(idx);
        } else {
            trim--;
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
            // append to tail
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength, reader);
        }
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;

    }

    protected void genReadBytesDelta(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
        int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim >= 0) {
            // append to tail
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength, reader);
        } else {
            // append to head
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength, reader);
        }
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    
    protected void genReadBytesTail(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        int length = PrimitiveReader.readIntegerUnsigned(reader);
        
        // append to tail
        int targetOffset = byteHeap.makeSpaceForAppend(idx, trim, length);
        PrimitiveReader.readByteData(byteHeap.rawAccess(), targetOffset, length, reader);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    
    protected void genReadBytesNoneOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int length = PrimitiveReader.readIntegerUnsigned(reader) - 1;
        PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadBytesNone(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int length = PrimitiveReader.readIntegerUnsigned(reader) - 0;
        PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    // dictionary reset

    protected void genReadDictionaryBytesReset(int idx, ByteHeap byteHeap) {
        byteHeap.setNull(idx);
    }

    protected void genReadDictionaryTextReset(int idx, TextHeap textHeap) {
        textHeap.reset(idx);
    }

    protected void genReadDictionaryLongReset(int idx, long resetConst, long[] rLongDictionary) {
        rLongDictionary[idx] = resetConst;
    }

    protected void genReadDictionaryIntegerReset(int idx, int resetConst, int[] rIntDictionary) {
        rIntDictionary[idx] = resetConst;
    }

    //TODO: C, Need a way to stream to disk over gaps of time. Write FAST to a file and Write series of Dictionaries to another, this set is valid for 1 catalog.
    
    
}
