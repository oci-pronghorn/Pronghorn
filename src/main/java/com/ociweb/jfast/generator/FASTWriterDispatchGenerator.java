package com.ociweb.jfast.generator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.tools.JavaFileObject;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffer.PaddedLong;
import com.ociweb.pronghorn.ring.util.IntWriteOnceOrderedSet;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;

public class FASTWriterDispatchGenerator extends FASTWriterInterpreterDispatch {
    
    private static final String ENTRY_METHOD_NAME = "encode";
    private final GeneratorData generatorData;
    private final List<JavaFileObject> alsoCompileTarget;
    

    public FASTWriterDispatchGenerator(byte[] catBytes, TemplateCatalogConfig catalog, List<JavaFileObject> alsoCompileTarget) {
        super(catalog );

        this.generatorData = new GeneratorData(catBytes, FASTWriterDispatchTemplates.class);
        this.alsoCompileTarget = alsoCompileTarget;
    }
        
    
    public <T extends Appendable> T generateFullSource(T target) throws RuntimeException {
    	IntWriteOnceOrderedSet doneScripts = new IntWriteOnceOrderedSet(17);
        List<String> doneScriptsParas = new ArrayList<String>(1<<17);
        
        try {
		    GeneratorUtils.generateHead(generatorData, target, FASTClassLoader.SIMPLE_WRITER_NAME, FASTEncoder.class.getSimpleName());
		    GeneratorUtils.buildGroupMethods(new TemplateCatalogConfig(generatorData.origCatBytes),doneScripts,doneScriptsParas,target, this, generatorData, alsoCompileTarget);        
		    GeneratorUtils.buildEntryDispatchMethod(preambleData.length,doneScripts,doneScriptsParas,target,ENTRY_METHOD_NAME, PrimitiveWriter.class, generatorData);
		    GeneratorUtils.generateTail(generatorData, target);
        }  catch (IOException ioex) {
        	throw new RuntimeException(ioex);
        }
        return target;
    }

    @Override
    protected void genWriteCopyBytes(int source, int target, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, source, target);
    }


    @Override
    protected void genWritePreamble(int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
    }

    
    @Override
    protected void genWriteTextDefaultOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
        
    }


    @Override
    protected void genWriteTextCopyOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteTextDeltaOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteTextTailOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteNull(PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextDefault(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
        
    }


    @Override
    protected void genWriteTextCopy(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteTextDelta(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteTextTail(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteTextNone(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
        
    }
    
    @Override
    protected void genWriteTextNoneOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
        
    }

    @Override
    protected void genWriteTextConstantOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
        
    }

    @Override
    protected void genWriteBytesDefault(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
        
    }


    @Override
    protected void genWriteBytesCopy(int target, int fieldPos, LocalHeap byteHeap, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    public void genWriteBytesDelta(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    public void genWriteBytesTail(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteBytesNone(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
        
    }


    @Override
    public void genWriteBytesDefaultOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
        
    }


    @Override
    public void genWriteBytesCopyOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    public void genWriteBytesDeltaOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteBytesConstantOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos);
        
    }


    @Override
    public void genWriteBytesTailOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteBytesNoneOptional(int target, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerSignedDefault(int constDefault, int fieldPos, PrimitiveWriter writer,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constDefault, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerSignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer,
            int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerSignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer,
            int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerSignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer,
            int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerSignedNone(int target, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerUnsignedDefault(int constDefault, int fieldPos, PrimitiveWriter writer,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constDefault, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerUnsignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer,
            int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerUnsignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer,
            int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerUnsignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer,
            int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerUnsignedNone(int target, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);        
    }

    //

    @Override
    protected void genWriteIntegerSignedDefaultOptional(int source, int fieldPos, int constDefault,
            int valueOfNull, PrimitiveWriter writer,  int[] rbB, int rbMask, PaddedLong rbPos, int[] rIntDictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, source, fieldPos, constDefault, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerSignedIncrementOptional(int target, int source, int fieldPos,
            int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary,  int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerSignedCopyOptional(int target, int source, int fieldPos, int valueOfNull,
            PrimitiveWriter writer, int[] rIntDictionary,  int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerSignedConstantOptional(int valueOfNull, int fieldPos, PrimitiveWriter writer,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, fieldPos);
        
    }


    @Override
    protected void genWriteIntegerSignedDeltaOptional(int target, int source, int fieldPos, int valueOfNull,
            PrimitiveWriter writer, int[] rIntDictionary,  int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerSignedNoneOptional(int target, int fieldPos, int valueOfNull,
            PrimitiveWriter writer, int[] rIntDictionary,  int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedCopyOptional(int target, int source, int fieldPos, int valueOfNull,
            PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedDefaultOptional(int source, int fieldPos, int valueOfNull,
            int constDefault, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, int[] rIntDictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, source, fieldPos, valueOfNull, constDefault);
        
    }


    @Override
    protected void genWriteIntegerUnsignedIncrementOptional(int target, int source, int fieldPos,
            int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedConstantOptional(int fieldPos, int valueOfNull, PrimitiveWriter writer,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, fieldPos, valueOfNull);
        
    }

    @Override
    protected void genWriteIntegerUnsignedDeltaOptional(int target, int source, int fieldPos,
            int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedNoneOptional(int target, int valueOfNull, int fieldPos, PrimitiveWriter writer,
            int[] rbB, int rbMask, PaddedLong rbPos, int[] rIntDictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, valueOfNull, fieldPos);
        
    }

    @Override
    protected void genWriteDecimalDefaultOptionalNone(int exponentSource, int mantissaTarget, int exponentConstDefault,
            int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary,
            int[] rIntDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, fieldPos);

    }


    @Override
    protected void genWriteDecimalIncrementOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget,
            int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer,
            long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalCopyOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget,
            int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer,
            long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalConstantOptionalNone(int exponentValueOfNull, int mantissaTarget, int fieldPos,
            PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaTarget, fieldPos);
        
    }

    @Override
    protected void genWriteDecimalDeltaOptionalNone(int exponentTarget, int mantissaTarget, int exponentSource,
            int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer,
            long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalNoneOptionalNone(int exponentTarget, int mantissaTarget, int exponentValueOfNull,
            int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaTarget, exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalDefault(int exponentSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, long mantissaConstDefault, int fieldPos,
            PrimitiveWriter writer, RingBuffer rbRingBuffer, int[] rIntDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, mantissaConstDefault, fieldPos);
    }
    


    @Override
    protected void genWriteDecimalIncrementOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget,
            int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull,
                mantissaConstDefault, fieldPos);
    }


    @Override
    protected void genWriteDecimalCopyOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget,
            int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull,
                mantissaConstDefault, fieldPos);

    }

    @Override
    protected void genWriteDecimalConstantOptionalDefault(int exponentValueOfNull, int mantissaTarget,
            long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentValueOfNull, mantissaTarget, mantissaConstDefault, fieldPos);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalDefault(int exponentTarget, int mantissaTarget, int exponentSource,
            int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull,
                mantissaConstDefault, fieldPos);
    }


    @Override
    protected void genWriteDecimalNoneOptionalDefault(int exponentTarget, int mantissaTarget, int exponentValueOfNull,
            long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaTarget, exponentValueOfNull, mantissaConstDefault, fieldPos);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalIncrement(int exponentSource, int mantissaSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer,
            RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaSource, mantissaTarget, exponentConstDefault,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalIncrementOptionalIncrement(int exponentTarget, int exponentSource,
            int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer,
            int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalCopyOptionalIncrement(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalConstantOptionalIncrement(int exponentValueOfNull, int mantissaSource,
            int mantissaTarget, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaSource, mantissaTarget, fieldPos);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentSource,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalNoneOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalCopy(int exponentSource, int mantissaSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer,
            RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaSource, mantissaTarget, exponentConstDefault,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalIncrementOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalCopyOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalConstantOptionalCopy(int exponentValueOfNull, int mantissaSource, int mantissaTarget,
            int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaSource, mantissaTarget, fieldPos);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentSource,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalNoneOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalConstant(int exponentSource, int mantissaSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer,
            RingBuffer rbRingBuffer, int[] rIntDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaSource, mantissaTarget, exponentConstDefault,
                exponentValueOfNull, fieldPos);
    }
    

    @Override
    protected void genWriteDecimalIncrementOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalCopyOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalConstantOptionalConstant(int exponentValueOfNull, int mantissaSource,
            int mantissaTarget, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaSource, mantissaTarget, fieldPos);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentSource,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalNoneOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalDelta(int exponentSource, int mantissaSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer,
            RingBuffer rbRingBuffer, long[] rLongDictionary, int[] rIntDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaSource, mantissaTarget, exponentConstDefault,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalIncrementOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalCopyOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, long[] longValue, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull, fieldPos);
    }

//
    @Override
    protected void genWriteDecimalConstantOptionalDelta(int exponentValueOfNull, int mantissaSource,
            int mantissaTarget, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] longValue, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaSource, mantissaTarget, fieldPos);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary,
            RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentSource,
                exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteDecimalNoneOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, fieldPos);
    }


    @Override
    protected void genWriteLongUnsignedDefault(long constDefault, int fieldPos, PrimitiveWriter writer,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constDefault, fieldPos);
        
    }
   

    @Override
    protected void genWriteLongUnsignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongUnsignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongUnsignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongUnsignedNone(int target, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }


    @Override
    protected void genWriteLongUnsignedDefaultOptional(long valueOfNull, int target, long constDefault, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, long[] rLongDictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, constDefault, fieldPos); 
        
    }


    @Override
    protected void genWriteLongUnsignedIncrementOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongUnsignedCopyOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongUnsignedConstantOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, fieldPos);
        
    }


    @Override
    protected void genWriteLongUnsignedNoneOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, fieldPos);
        
    }


    @Override
    protected void genWriteLongUnsignedDeltaOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongSignedDefault(long constDefault, int fieldPos, PrimitiveWriter writer,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constDefault, fieldPos);
        
    }


    @Override
    protected void genWriteLongSignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongSignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongSignedNone(int target, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary,
            int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos);
        
    }

    @Override
    protected void genWriteLongSignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongSignedOptional(long valueOfNull, int target,  int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, fieldPos);
        
    }

    @Override
    protected void genWriteLongSignedDeltaOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, source, fieldPos);
        
    }


    @Override
    protected void genWriteLongSignedConstantOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, fieldPos);
        
    }


    @Override
    protected void genWriteLongSignedCopyOptional(int target, int source, long valueOfNull, int fieldPos, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, valueOfNull, fieldPos);
        
    }


    @Override
    protected void genWriteLongSignedIncrementOptional(int target, int source, int fieldPos, long valueOfNull, PrimitiveWriter writer,
            long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, fieldPos, valueOfNull);
        
    }


    @Override
    protected void genWriteLongSignedDefaultOptional(int target, int fieldPos, long valueOfNull, long constDefault, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, long[] rLongDictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, fieldPos, valueOfNull, constDefault);
        
    }


    @Override
    protected void genWriteDictionaryBytesReset(int target, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteDictionaryTextReset(int target, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteDictionaryLongReset(int target, long constValue, long[] rLongDictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, constValue);
        
    }


    @Override
    protected void genWriteDictionaryIntegerReset(int target, int constValue, int[] rIntDictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, constValue);
        
    }

    
    @Override
    protected void genWriteClosePMap(PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteCloseTemplatePMap(PrimitiveWriter writer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteCloseTemplate(PrimitiveWriter writer, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteOpenTemplatePMap(int pmapSize, int fieldPos, int msgIdx, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, FASTEncoder dispatch) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, pmapSize, fieldPos, msgIdx);
        
    }


    @Override
    protected void genWriteOpenGroup(int pmapSize, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, pmapSize);
        
    }

    
}
