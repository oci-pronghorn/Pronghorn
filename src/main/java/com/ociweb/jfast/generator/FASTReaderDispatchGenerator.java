package com.ociweb.jfast.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.tools.JavaFileObject;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffer.PaddedLong;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.util.IntWriteOnceOrderedSet;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;

//This generator makes use of class names whenever possible to allow for obfuscation if needed.
public class FASTReaderDispatchGenerator extends FASTReaderInterpreterDispatch {

    // TODO: C, code does not support final in signatures, this would be nice to have
    
    private static final String ENTRY_METHOD_NAME = "decode";
    private final GeneratorData generatorData;
    private final List<JavaFileObject> alsoCompileTarget;

    public FASTReaderDispatchGenerator(byte[] catBytes, List<JavaFileObject> alsoCompileTarget, RingBuffers ringBuffers) {
        super(catBytes,ringBuffers);
        this.generatorData = new GeneratorData(catBytes,FASTReaderDispatchTemplates.class);
        this.alsoCompileTarget = alsoCompileTarget;
    }
        
    
    public <T extends Appendable> T generateFullSource(T target) throws RuntimeException {
    	IntWriteOnceOrderedSet doneScripts = new IntWriteOnceOrderedSet(17);
        List<String> doneScriptsParas = new ArrayList<String>(1<<17);
                
        try {
	        GeneratorUtils.generateHead(generatorData, target, FASTClassLoader.SIMPLE_READER_NAME, FASTDecoder.class.getSimpleName());
	        GeneratorUtils.buildGroupMethods(new TemplateCatalogConfig(generatorData.origCatBytes),doneScripts,doneScriptsParas,target, this, generatorData, alsoCompileTarget);       
	        GeneratorUtils.buildEntryDispatchMethod(prembleBytes,doneScripts,doneScriptsParas,target,ENTRY_METHOD_NAME, PrimitiveReader.class,generatorData);
	        GeneratorUtils.generateTail(generatorData, target);
        } catch (IOException ioex) {
        	throw new RuntimeException(ioex);
        }
        return target;
    }

    @Override
    protected void genReadGroupCloseMessage(PrimitiveReader reader, FASTDecoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }
    
    
    @Override
    protected void genReadTemplateId(int preambleDataLength, int maxTemplatePMapSize, PrimitiveReader reader, FASTDecoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, preambleDataLength, maxTemplatePMapSize);
    }

    @Override
    protected void genWriteTemplateId(FASTDecoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }

    @Override
    protected void genWritePreambleB(FASTDecoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }

    @Override
    protected void genWritePreambleA(FASTDecoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }

    @Override
    protected void genReadPreambleB(PrimitiveReader reader, FASTDecoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }

    @Override
    protected void genReadPreambleA(PrimitiveReader reader, FASTDecoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }
    
    @Override
    protected void genReadTotalMessageBytesUsed(PaddedLong rbPos, RingBuffer rbRingBuffer) {    	
    	GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }
   
    @Override
    protected void genReadTotalMessageBytesResetUsed(RingBuffer rbRingBuffer) {    	
    	GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }
    
    @Override
    protected void genReadSequenceClose(int topCursorPos, FASTDecoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, topCursorPos);
    }
    
    @Override
    protected void genReadGroupPMapOpen(int nonTemplatePMapSize, PrimitiveReader reader) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, nonTemplatePMapSize);
    }
    
    @Override
    protected void genReadGroupClose(PrimitiveReader reader) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }
    
    
    // length methods
    
    @Override
    protected int genReadLengthDefault(int constDefault,  int jumpToTarget, int jumpToNext, int[] rbB, PrimitiveReader reader, int rbMask, PaddedLong rbPos, FASTDecoder dispatch) {
        //generatorData.sequenceStarts.add(activeScriptCursor+1);
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,constDefault, jumpToTarget, jumpToNext);
        return jumpToNext;
    }

    @Override
    protected void genReadLengthIncrement(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos, PrimitiveReader reader, FASTDecoder dispatch) {
        //generatorData.sequenceStarts.add(activeScriptCursor+1);
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target,source, jumpToTarget, jumpToNext);
    }

    @Override
    protected void genReadLengthCopy(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos, PrimitiveReader reader, FASTDecoder dispatch) {
        //generatorData.sequenceStarts.add(activeScriptCursor+1);
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target,source, jumpToTarget, jumpToNext);
    }

    @Override
    protected void genReadLengthConstant(int constDefault, int jumpToTarget, int jumpToNext, int[] rbB, int rbMask, PaddedLong rbPos, FASTDecoder dispatch) {
        //generatorData.sequenceStarts.add(activeScriptCursor+1);
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,constDefault, jumpToTarget, jumpToNext);
    }

    @Override
    protected void genReadLengthDelta(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos, PrimitiveReader reader, FASTDecoder dispatch) {
        //generatorData.sequenceStarts.add(activeScriptCursor+1);
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target,source, jumpToTarget, jumpToNext);
    }

    @Override
    protected void genReadLength(int target,  int jumpToTarget, int jumpToNext, int[] rbB, int rbMask, PaddedLong rbPos, int[] rIntDictionary, PrimitiveReader reader, FASTDecoder dispatch) {
        //generatorData.sequenceStarts.add(activeScriptCursor+1);
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this,target, jumpToTarget, jumpToNext);
    }
    
    // copy methods
    
    @Override
    protected void genReadCopyBytes(int source, int target, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, source, target);
    }
    
    // int methods

    @Override
    protected void genReadIntegerUnsignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constAbsent, constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrementOptionalTS(int targsrc, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,targsrc, constAbsent);
    }

    @Override
    protected void genReadIntegerUnsignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constAbsent, constConst);
    }
    
    @Override
    protected void genReadIntegerUnsignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrementTS(int targsrc, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, targsrc);
    }
    
    @Override
    protected void genReadIntegerUnsignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadIntegerUnsignedCopyTS(int target, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target);
    }
    
    @Override
    protected void genReadIntegerUnsignedConstant(int constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadIntegerUnsigned(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target);
    }
    
    @Override
    protected void genReadIntegerSignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadIntegerSignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadIntegerConstant(int constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadIntegerSignedNone(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target);
    }
    
    @Override
    protected void genReadIntegerSignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constAbsent, constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constAbsent, constConst);
    }
    
    @Override
    protected void genReadIntegerSignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent);
    }
    
    // long methods
    
    @Override
    protected void genReadLongUnsignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedIncrement(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadLongUnsignedCopy(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadLongConstant(long constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedDelta(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadLongUnsignedNone(int target, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target);
    }
    
    @Override
    protected void genReadLongUnsignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constAbsent, constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }

    @Override
    protected void genReadLongUnsignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadLongUnsignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constAbsent, constConst);
    }
    
    @Override
    protected void genReadLongUnsignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadLongUnsignedOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent);
    }
    
    @Override
    protected void genReadLongSignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constDefault);
    }
    
    @Override
    protected void genReadLongSignedIncrement(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadLongSignedCopy(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadLongSignedConstant(long constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constDefault);
    }
    
    @Override
    protected void genReadLongSignedDelta(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, target, source);
    }
    
    @Override
    protected void genReadLongSignedDeltaTS(int targsrc, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, targsrc);
    }
        
    @Override
    protected void genReadLongSignedNone(int target, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target);
    }
    
    @Override
    protected void genReadLongSignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constAbsent, constDefault);
    }
    
    @Override
    protected void genReadLongSignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadLongSignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadLongSignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constAbsent, constConst);
    }
    
    @Override
    protected void genReadLongSignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,target, source, constAbsent);
    }
    
    @Override
    protected void genReadLongSignedNoneOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent);
    }

    // text methods.

    @Override
    protected void genReadASCIITail(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadTextConstant(int constIdx, int constLen, int[] rbB, int rbMask, int bytesBasePos, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constIdx, constLen);
    }
    
    @Override
    protected void genReadASCIIDelta(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadASCIICopy(int idx, int rbMask, int[] rbB, PrimitiveReader reader, LocalHeap byteHeap, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadASCIINone(int idx, int[] rbB, int rbMask, PrimitiveReader reader, LocalHeap byteHeap, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadASCIITailOptional(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadASCIIDeltaOptional(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadTextConstantOptional(int constInit, int constValue, int constInitLen, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, int bytesBasePos, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,constInit,constValue, constInitLen, bytesBasePos, constValueLen);
    }
    
    @Override
    protected void genReadASCIICopyOptional(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadASCIIDefault(int idx, int defIdx, int defLen, int rbMask, int[] rbB, PrimitiveReader reader, LocalHeap byteHeap, PaddedLong rbPos, byte[] byteBuffer, int byteMask, RingBuffer rbRingBuffer, int bytesBasePos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,idx, defIdx, defLen);
    }
    
    //byte methods
    
    @Override
    protected void genReadBytesConstant(int constIdx, int constLen, int[] rbB, int rbMask, int bytesBasePos, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, constIdx, constLen);
    }
    
    @Override
    protected void genReadBytesConstantOptional(int constInit, int constInitLen, int constValue, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, int bytesBasePos, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this,constInit,constInitLen, constValue, constValueLen);
    }
    
    @Override
    protected void genReadBytesDefault(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer, int bytesHeadPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, idx,defIdx, defLen, optOff);
    }
    
    @Override
    protected void genReadBytesCopy(int idx, int optOff, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData,this, idx, optOff);
    }
    
    @Override
    protected void genReadBytesDeltaOptional(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadBytesTailOptional(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadBytesDelta(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadBytesTail(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadBytesNoneOptional(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer, int bytesHeadPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadBytesNone(int idx, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }

    // dictionary reset
    
    @Override
    protected void genReadDictionaryBytesReset(int idx, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadDictionaryTextReset(int idx, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx);
    }
    
    @Override
    protected void genReadDictionaryLongReset(int idx, long resetConst, long[] rLongDictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx, resetConst);
    }
    
    @Override
    protected void genReadDictionaryIntegerReset(int idx, int resetConst, int[] rIntDictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, idx, resetConst);
    }

    //decimals    

    @Override
    protected void genReadDecimalDefaultOptionalMantissaDefault(int constAbsent, int constDefault,
            long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constAbsent, constDefault, mantissaConstDefault);

    }


    @Override
    protected void genReadDecimalIncrementOptionalMantissaDefault(int target, int source, int constAbsent,
            long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, constAbsent, mantissaConstDefault);

    }


    @Override
    protected void genReadDecimalCopyOptionalMantissaDefault(int target, int source, int constAbsent,
            long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos) {

        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalConstantOptionalMantissaDefault(int constAbsent, int constConst,
            long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constConst, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalDeltaOptionalMantissaDefault(int target, int source, int constAbsent,
            long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalOptionalMantissaDefault(int constAbsent, long mantissaConstDefault, int[] rbB,
            int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalDefaultOptionalMantissaIncrement(int constAbsent, int constDefault,
            int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constDefault, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalIncrementOptionalMantissaIncrement(int target, int source, int constAbsent,
            int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask,
            PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalCopyOptionalMantissaIncrement(int target, int source, int constAbsent,
            int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask,
            PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalConstantOptionalMantissaIncrement(int constAbsent, int constConst, int mantissaTarget,
            int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constConst, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalDeltaOptionalMantissaIncrement(int target, int source, int constAbsent,
            int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask,
            PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalOptionalMantissaIncrement(int constAbsent, int mantissaTarget, int mantissaSource,
            int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalDefaultOptionalMantissaCopy(int constAbsent, int constDefault, int mantissaTarget,
            int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constDefault, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalIncrementOptionalMantissaCopy(int target, int source, int constAbsent,
            int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask,
            PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalCopyOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget,
            int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalConstantOptionalMantissaCopy(int constAbsent, int constConst, int mantissaTarget,
            int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constConst, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalDeltaOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget,
            int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalOptionalMantissaCopy(int constAbsent, int mantissaTarget, int mantissaSource,
            int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalDefaultOptionalMantissaConstant(int constAbsent, int constDefault,
            long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constDefault, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalIncrementOptionalMantissaConstant(int target, int source, int constAbsent,
            long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalCopyOptionalMantissaConstant(int target, int source, int constAbsent,
            long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalConstantOptionalMantissaConstant(int constAbsent, int constConst,
            long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constConst, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalDeltaOptionalMantissaConstant(int target, int source, int constAbsent,
            long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalOptionalMantissaConstant(int constAbsent, long mantissaConstDefault, int[] rbB,
            int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, mantissaConstDefault);
    }


    @Override
    protected void genReadDecimalDefaultOptionalMantissaDelta(int constAbsent, int constDefault, int mantissaTarget,
            int mantissaSource, int[] rbB, long[] rLongDictionary, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constDefault, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalIncrementOptionalMantissaDelta(int target, int source, int constAbsent,
            int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask,
            PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalCopyOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget,
            int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalConstantOptionalMantissaDelta(int constAbsent, int constConst, int mantissaTarget,
            int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constConst, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalDeltaOptionalMantissaDelta(int target, int source, int constAbsent,
            int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask,
            PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalOptionalMantissaDelta(int constAbsent, int mantissaTarget, int mantissaSource,
            int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, mantissaTarget, mantissaSource);
    }


    @Override
    protected void genReadDecimalDefaultOptionalMantissaNone(int constAbsent, int constDefault, int mantissaTarget,
            int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constDefault, mantissaTarget);
    }


    @Override
    protected void genReadDecimalIncrementOptionalMantissaNone(int target, int source, int constAbsent,
            int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader,
            PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget);
    }


    @Override
    protected void genReadDecimalCopyOptionalMantissaNone(int target, int source, int constAbsent, int mantissaTarget,
            int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget);
    }


    @Override
    protected void genReadDecimalConstantOptionalMantissaNone(int constAbsent, int constConst, int mantissaTarget,
            int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, constConst, mantissaTarget);
    }


    @Override
    protected void genReadDecimalDeltaOptionalMantissaNone(int target, int source, int constAbsent, int mantissaTarget,
            int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, target, source, constAbsent, mantissaTarget);
    }


    @Override
    protected void genReadDecimalOptionalMantissaNone(int constAbsent, int mantissaTarget, int[] rbB, int rbMask,
            PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        
        GeneratorUtils.generator(new Exception().getStackTrace(),generatorData, this, constAbsent, mantissaTarget);
    }
    


    
}
