package com.ociweb.jfast.generator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;

public class FASTWriterDispatchGenerator extends FASTWriterInterpreterDispatch {
    
    private static final String ENTRY_METHOD_NAME = "encode";
    private final GeneratorData generatorData;

    public FASTWriterDispatchGenerator(byte[] catBytes) {
        super(new TemplateCatalogConfig(catBytes));

        generatorData = new GeneratorData(catBytes, FASTWriterDispatchTemplates.class);

    }
        
    
    public <T extends Appendable> T generateFullReaderSource(T target) throws IOException {
        List<Integer> doneScripts = new ArrayList<Integer>();
        List<String> doneScriptsParas = new ArrayList<String>();
        
        GeneratorUtils.generateHead(generatorData.templates, generatorData.origCatBytes, target, FASTClassLoader.SIMPLE_WRITER_NAME, FASTEncoder.class.getSimpleName());
        GeneratorUtils.buildGroupMethods(new TemplateCatalogConfig(generatorData.origCatBytes),doneScripts,doneScriptsParas,target, this, generatorData);
               
        GeneratorUtils.buildEntryDispatchMethod(doneScripts,doneScriptsParas,target,ENTRY_METHOD_NAME, PrimitiveWriter.class);
        GeneratorUtils.generateTail(target);
        
        return target;
    }


    @Override
    protected void genWriteCopyText(int source, int target, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, source, target);
    }


    @Override
    protected void genWriteCopyBytes(int source, int target, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, source, target);
    }


    @Override
    protected void genWritePreamble(byte[] preambleData, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
    }


    @Override
    protected void genWriteUTFTextDefaultOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
    }


    @Override
    protected void genWriteUTFTextCopyOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
    }


    @Override
    protected void genWriteUTFTextDeltaOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteUTFTextConstantOptional(PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteUTFTextTailOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteUTFTextNoneOptional(CharSequence value, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteUTFTextDefault(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteUTFTextCopy(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteUTFTextDelta(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteUTFTextTail(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteUTFTextNone(CharSequence value, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextDefaultOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextCopyOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextDeltaOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextTailOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteNull(PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextDefault(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextCopy(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextDelta(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextTail(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextNone(CharSequence value, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextUTFDefaultOptional(int target, int offset, int length, char[] value,
            PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextUTFCopyOptional(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextUTFDeltaOptional(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextUTFConstantOptional(PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextUTFTailOptional(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextUTFNoneOptional(int offset, int length, char[] value, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextUTFDefault(int constId, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constId);
        
    }


    @Override
    protected void genWriteTextUTFCopy(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextUTFDelta(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextUTFTail(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextUTFNone(int offset, int length, char[] value, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextDefaultOptional(int constId, int offset, int length, char[] value,
            PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constId);
        
    }
    
    @Override
    protected void genWriteTextCopyOptional(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextDeltaOptional2(int target, int offset, int length, char[] value, TextHeap textHeap,
            PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
    }


    @Override
    protected void genWriteTextConstantOptional(PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextTailOptional2(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextNoneOptional(char[] value, int offset, int length, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteTextDefault2(int target, int offset, int length, char[] value, TextHeap textHeap,
            PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextCopy2(int target, int offset, int length, char[] value, TextHeap textHeap,
            PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextDelta2(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextTail2(int target, int offset, int length, char[] value, PrimitiveWriter writer,
            TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteTextNone(char[] value, int offset, int length, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteBytesDefault(int target, int offset, int length, byte[] value, LocalHeap byteHeap,
            PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteBytesCopy(int target, int offset, int length, byte[] value, LocalHeap byteHeap,
            PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteBytesDelta(int target, int offset, int length, byte[] value, PrimitiveWriter writer,
            LocalHeap byteHeap, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteBytesTail(int target, int offset, int length, byte[] value, PrimitiveWriter writer,
            LocalHeap byteHeap, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteBytesNone(int offset, int length, byte[] value, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    public void genWriteBytesDefaultOptional(int target, int offset, int length, byte[] value, PrimitiveWriter writer,
            LocalHeap byteHeap, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteBytesCopyOptional(int target, int offset, int length, byte[] value, PrimitiveWriter writer,
            LocalHeap byteHeap, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteBytesDeltaOptional(int target, int offset, int length, byte[] value, PrimitiveWriter writer,
            LocalHeap byteHeap, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteBytesConstantOptional(PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    public void genWriteBytesTailOptional(int target, int offset, int length, byte[] value, PrimitiveWriter writer,
            LocalHeap byteHeap, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteBytesNoneOptional(int offset, int length, byte[] value, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }


    @Override
    protected void genWriteIntegerSignedDefault(int constDefault, PrimitiveWriter writer, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constDefault);
        
    }


    @Override
    protected void genWriteIntegerSignedIncrement(int target, int source, PrimitiveWriter writer, int[] intValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteIntegerSignedCopy(int target, int source, PrimitiveWriter writer, int[] intValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteIntegerSignedDelta(int target, int source, PrimitiveWriter writer, int[] intValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteIntegerSignedNone(int target, PrimitiveWriter writer, int[] intValues, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteIntegerUnsignedDefault(int constDefault, int rbPos, PrimitiveWriter writer,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constDefault);
        
    }


    @Override
    protected void genWriteIntegerUnsignedIncrement(int target, int source, int rbPos, PrimitiveWriter writer,
            int[] intValues, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteIntegerUnsignedCopy(int target, int source, int rbPos, PrimitiveWriter writer,
            int[] intValues, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteIntegerUnsignedDelta(int target, int source, int rbPos, PrimitiveWriter writer,
            int[] intValues, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteIntegerUnsignedNone(int target, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteIntegerSignedDefaultOptional(int source, int constDefault, int valueOfNull,
            PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, source, constDefault, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerSignedIncrementOptional(int target, int source, int valueOfNull,
            PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerSignedCopyOptional(int target, int source, int valueOfNull, PrimitiveWriter writer,
            int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerSignedConstantOptional(int valueOfNull, PrimitiveWriter writer, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerSignedDeltaOptional(int target, int source, int valueOfNull, PrimitiveWriter writer,
            int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerSignedNoneOptional(int target, int valueOfNull, PrimitiveWriter writer,
            int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedCopyOptional(int target, int source, int valueOfNull, PrimitiveWriter writer,
            int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedDefaultOptional(int source, int valueOfNull, int constDefault,
            PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, source, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedIncrementOptional(int target, int source, int valueOfNull,
            PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedConstantOptional(int valueOfNull, PrimitiveWriter writer, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedDeltaOptional(int target, int source, int valueOfNull,
            PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source, valueOfNull);
        
    }


    @Override
    protected void genWriteIntegerUnsignedNoneOptional(int target, int valueOfNull, PrimitiveWriter writer, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, valueOfNull);
        
    }

    @Override
    protected void genWriteDecimalDefaultOptionalNone(int exponentSource, int mantissaTarget, int exponentConstDefault,
            int exponentValueOfNull, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues,
            int[] intValues) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull);

    }


    @Override
    protected void genWriteDecimalIncrementOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget,
            int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer,
            long[] longValues) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalCopyOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget,
            int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer,
            long[] longValues) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalConstantOptionalNone(int exponentValueOfNull, int mantissaTarget, int rbPos,
            PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaTarget);
        
    }

    @Override
    protected void genWriteDecimalDeltaOptionalNone(int exponentTarget, int mantissaTarget, int exponentSource,
            int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer,
            long[] longValues) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalNoneOptionalNone(int exponentTarget, int mantissaTarget, int exponentValueOfNull,
            int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaTarget, exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalDefault(int exponentSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, long mantissaConstDefault, int rbPos,
            PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, mantissaConstDefault);
    }
    


    @Override
    protected void genWriteDecimalIncrementOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget,
            int exponentValueOfNull, long mantissaConstDefault, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull,
                mantissaConstDefault);
    }


    @Override
    protected void genWriteDecimalCopyOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget,
            int exponentValueOfNull, long mantissaConstDefault, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull,
                mantissaConstDefault);

    }

    @Override
    protected void genWriteDecimalConstantOptionalDefault(int exponentValueOfNull, int mantissaTarget,
            long mantissaConstDefault, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentValueOfNull, mantissaTarget, mantissaConstDefault);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalDefault(int exponentTarget, int mantissaTarget, int exponentSource,
            int exponentValueOfNull, long mantissaConstDefault, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull,
                mantissaConstDefault);
    }


    @Override
    protected void genWriteDecimalNoneOptionalDefault(int exponentTarget, int mantissaTarget, int exponentValueOfNull,
            long mantissaConstDefault, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaTarget, exponentValueOfNull, mantissaConstDefault);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalIncrement(int exponentSource, int mantissaSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaSource, mantissaTarget, exponentConstDefault,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalIncrementOptionalIncrement(int exponentTarget, int exponentSource,
            int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer,
            int[] intValues, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalCopyOptionalIncrement(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalConstantOptionalIncrement(int exponentValueOfNull, int mantissaSource,
            int mantissaTarget, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaSource, mantissaTarget);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentSource,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalNoneOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalCopy(int exponentSource, int mantissaSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaSource, mantissaTarget, exponentConstDefault,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalIncrementOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalCopyOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalConstantOptionalCopy(int exponentValueOfNull, int mantissaSource, int mantissaTarget,
            int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaSource, mantissaTarget);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentSource,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalNoneOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalConstant(int exponentSource, int mantissaSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaSource, mantissaTarget, exponentConstDefault,
                exponentValueOfNull);
    }
    

    @Override
    protected void genWriteDecimalIncrementOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this,exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalCopyOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalConstantOptionalConstant(int exponentValueOfNull, int mantissaSource,
            int mantissaTarget, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaSource, mantissaTarget);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentSource,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalNoneOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalDefaultOptionalDelta(int exponentSource, int mantissaSource, int mantissaTarget,
            int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentSource, mantissaSource, mantissaTarget, exponentConstDefault,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalIncrementOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalCopyOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource,
            int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, exponentSource, mantissaSource, mantissaTarget,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalConstantOptionalDelta(int exponentValueOfNull, int mantissaSource,
            int mantissaTarget, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentValueOfNull, mantissaSource, mantissaTarget);
    }


    @Override
    protected void genWriteDecimalDeltaOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentSource,
                exponentValueOfNull);
    }


    @Override
    protected void genWriteDecimalNoneOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget,
            int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull);
    }


    @Override
    protected void genWriteLongUnsignedDefault(long constDefault, PrimitiveWriter writer, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constDefault);
        
    }
   

    @Override
    protected void genWriteLongUnsignedIncrement(int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteLongUnsignedCopy(int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteLongUnsignedDelta(int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteLongUnsignedNone(int target, PrimitiveWriter writer, long[] longValues, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteLongUnsignedDefaultOptional(long valueOfNull, int target, long constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, constDefault); 
        
    }


    @Override
    protected void genWriteLongUnsignedIncrementOptional(long valueOfNull, int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target);
        
    }


    @Override
    protected void genWriteLongUnsignedCopyOptional(long valueOfNull, int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, source);
        
    }


    @Override
    protected void genWriteLongUnsignedConstantOptional(long valueOfNull, int target, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target);
        
    }


    @Override
    protected void genWriteLongUnsignedNoneOptional(long valueOfNull, int target, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target);
        
    }


    @Override
    protected void genWriteLongUnsignedDeltaOptional(long valueOfNull, int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, source);
        
    }


    @Override
    protected void genWriteLongSignedDefault(long constDefault, PrimitiveWriter writer, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, constDefault);
        
    }


    @Override
    protected void genWriteLongSignedIncrement(int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteLongSignedCopy(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteLongSignedNone(int target, PrimitiveWriter writer, long[] longValues, int rbPos,
            FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteLongSignedDelta(int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteLongSignedOptional(long valueOfNull, int target,  PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer ringBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target);
        
    }

    @Override
    protected void genWriteLongSignedDeltaOptional(long valueOfNull, int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteLongSignedConstantOptional(long valueOfNull, int target, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target);
        
    }


    @Override
    protected void genWriteLongSignedCopyOptional(long valueOfNull, int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteLongSignedIncrementOptional(long valueOfNull, int target, int source, PrimitiveWriter writer, long[] longValues,
            int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, source);
        
    }


    @Override
    protected void genWriteLongSignedDefaultOptional(long valueOfNull, int target, long constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, valueOfNull, target, constDefault);
        
    }


    @Override
    protected void genWriteDictionaryBytesReset(int target, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteDictionaryTextReset(int target, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    protected void genWriteDictionaryLongReset(int target, long constValue, long[] longValues) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target, constValue);
        
    }


    @Override
    protected void genWriteDictionaryIntegerReset(int target, int constValue, int[] intValues) {
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
    protected void genWriteOpenTemplatePMap(int pmapSize, int fieldPos, PrimitiveWriter writer, FASTRingBuffer queue) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, pmapSize);
        
    }


    @Override
    protected void genWriteOpenGroup(int pmapSize, PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, pmapSize);
        
    }


    @Override
    public void genWriteNullPMap(PrimitiveWriter writer) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this);
        
    }

    
    @Override
    public void genWriteNullDefaultLong(int target, PrimitiveWriter writer, long[] dictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteNullCopyIncLong(int target, PrimitiveWriter writer, long[] dictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteNullNoPMapLong(int target, PrimitiveWriter writer, long[] dictionary) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteNullDefaultText(int target, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteNullCopyIncText(int target, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteNullNoPMapText(int target, PrimitiveWriter writer, TextHeap textHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteNullDefaultBytes(int target, PrimitiveWriter writer, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteNullNoPMapBytes(int target, PrimitiveWriter writer, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }


    @Override
    public void genWriteNullCopyIncBytes(int target, PrimitiveWriter writer, LocalHeap byteHeap) {
        GeneratorUtils.generator(new Exception().getStackTrace(), generatorData, this, target);
        
    }
    
}
