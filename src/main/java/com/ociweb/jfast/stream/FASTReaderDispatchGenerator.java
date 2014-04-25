package com.ociweb.jfast.stream;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.SourceTemplates;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FASTReaderDispatchGenerator extends FASTReaderDispatch {

    SourceTemplates templates;
    
    StringBuilder fieldBuilder;
    StringBuilder caseBuilder;
    String fieldPrefix;
    int fieldCount;
    
    String caseTail = "}\n";
    Set<Integer> sequenceStarts = new HashSet<Integer>();
    
    
    public FASTReaderDispatchGenerator(PrimitiveReader reader, DictionaryFactory dcr, int nonTemplatePMapSize,
            int[][] dictionaryMembers, int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript,
            int maxNestedGroupDepth, int primaryRingBits, int textRingBits) {
        
        super(reader, dcr, nonTemplatePMapSize, dictionaryMembers, maxTextLen, maxVectorLen, charGap, bytesGap, fullScript,
                maxNestedGroupDepth, primaryRingBits, textRingBits);
        
        templates = new SourceTemplates();
        fieldBuilder = new StringBuilder();
        caseBuilder = new StringBuilder();
        fieldCount = 0;
    }
    
    //This generator allows for refactoring of the NAME of these methods and the code generation will remain intact.
    
    public String getScriptBlock() {
        return caseBuilder.toString()+caseTail+fieldBuilder.toString();
    }
    
    public void startScriptBlock(int scriptPos, int templateId) {
        fieldBuilder.setLength(0);
        caseBuilder.setLength(0);
        
        //each field method will start with the templateId for easy debugging later.
        fieldPrefix = Integer.toString(templateId);
        while (fieldPrefix.length()<4) {
            fieldPrefix = "0"+fieldPrefix;
        }
        
        caseBuilder.append("private void case").append(scriptPos).append("() {     // for message ").append(templateId).append("\n");
        //add debug code
        caseBuilder.append("    assert (gatherReadData(reader, activeScriptCursor));\n");
        
        fieldPrefix = "m"+fieldPrefix;
    }
    
    public Set<Integer> getSequenceStarts() {
        return sequenceStarts;
    }
    
    private void generator(StackTraceElement[] trace, long ... values) {
        
        String methodNameKey = " "+trace[0].getMethodName()+'('; ///must include beginning and end to ensure match
        String[] params = templates.params(methodNameKey);
        String[] defs   = templates.defs(methodNameKey);
        String comment = "        //"+trace[0].getMethodName()+(Arrays.toString(params).replace('[','(').replace(']', ')'))+"\n";
        
        assert(params.length<values.length): "Bad params for "+methodNameKey;
        
        //replace variables with constants
        String template = templates.template(methodNameKey);
        long[] data = values;
        int i = data.length;
        while (--i>=0) {
            String strData; 
            if (data[i]>Integer.MAX_VALUE || 
                (data[i]<Integer.MIN_VALUE && (data[i]>>>32)!=0xFFFFFFFF)) {
                strData = "0x"+Long.toHexString(data[i])+"L";
            } else {
                strData = "0x"+Integer.toHexString((int)data[i]);
            }
            
            template = template.replace(params[i],strData+"/*"+params[i]+"*/");
        }
        
        
        StringBuilder fieldParaValues = new StringBuilder();
        StringBuilder fieldParaDefs = new StringBuilder();
        generateParameters(params, defs, fieldParaValues, fieldParaDefs, data.length);
        
        
        //replace ring buffer position increment
        template = template.replace("spclPosInc()", "rbRingBuffer.addPos++")+"\n"; //TODO: must remove this hardcoded reference somehow.
        
        fieldCount++;
        String field = Integer.toHexString(fieldCount);
        while (field.length()<3) {
            field = "0"+field;
        }
        field = fieldPrefix+"_"+field;
        
        if (methodNameKey.contains("Length")) {
            fieldBuilder.append("private boolean ");
            caseBuilder.append("    if (").append(field).append("(").append(fieldParaValues).append(")) {return;};\n");
        } else {
            fieldBuilder.append("private void ");
            caseBuilder.append("    ").append(field).append("(").append(fieldParaValues).append(");\n");
        }
        fieldBuilder.append(field).append("(").append(fieldParaDefs).append(") {\n").append(comment).append(template).append("};\n");
        
        //TODO: A, how is the read from supported, existing generator must get the from index value when in the script. not built into char/bytes yet.
    }

    private void generateParameters(String[] params, String[] defs, StringBuilder fieldParaValues,
            StringBuilder fieldParaDefs, int x) {
        /////////////
        ///generate params to be passed in to the method
        ///generate the param definitions in signature of each method
        ///these are the left over params from the gen method after removing values
        ///////////////
        while (x<params.length) {
            fieldParaValues.append(params[x]).append(',');
            fieldParaDefs.append(defs[x]).append(',');
            x++;
        }
        if (fieldParaValues.length()>0) {
            fieldParaValues.setLength(fieldParaValues.length()-1);
        }
        if (fieldParaDefs.length()>0) {
            fieldParaDefs.setLength(fieldParaDefs.length()-1);
        }
        //////////
        //////////
    }
    
    @Override
    protected void genReadSequenceClose(int backvalue) {
        generator(new Exception().getStackTrace(),backvalue);
    }
    
    @Override
    protected void genReadGroupPMapOpen(int nonTemplatePMapSize, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),nonTemplatePMapSize);
    }
    
    @Override
    protected void genReadGroupClose(PrimitiveReader reader) {
        generator(new Exception().getStackTrace());
    }
    
    
    // length methods
    
    @Override
    protected boolean genReadLengthDefault(int constDefault,  int jumpToTarget, int[] rbB) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),constDefault,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthIncrement(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target,source,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthCopy(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target,source,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthConstant(int constDefault, int jumpToTarget, int[] rbB) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),constDefault,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthDelta(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target,source,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLength(int target,  int jumpToTarget, int[] rbB) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target, jumpToTarget);
        return true;
    }
    
    // int methods

    @Override
    protected void genReadIntegerUnsignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }

    @Override
    protected void genReadIntegerUnsignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadIntegerUnsignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedDefault(int constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerUnsignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerUnsignedConstant(int constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedDelta(int target, int source, int[] rIntDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerUnsigned(int target, int[] rbB) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadIntegerSignedDefault(int constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerSignedCopy(int target, int source, int[] rIntDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerConstant(int constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedDelta(int target, int source, int[] rIntDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerSignedNone(int target, int[] rbB) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadIntegerSignedDefaultOptional(int constAbsent, int constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedConstantOptional(int constAbsent, int constConst, int[] rbB) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadIntegerSignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedOptional(int constAbsent, int[] rbB) {
        generator(new Exception().getStackTrace(),constAbsent);
    }

    // long methods
    
    @Override
    protected void genReadLongUnsignedDefault(long constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedIncrement(int target, int source, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongUnsignedCopy(int target, int source, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongConstant(long constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedDelta(int target, int source, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongUnsignedNone(int target, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadLongUnsignedDefaultOptional(long constAbsent, long constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }

    @Override
    protected void genReadLongUnsignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongUnsignedConstantOptional(long constAbsent, long constConst, int[] rbB) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadLongUnsignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongUnsignedOptional(long constAbsent, int[] rbB) {
        generator(new Exception().getStackTrace(),constAbsent);
    }
    
    @Override
    protected void genReadLongSignedDefault(long constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongSignedIncrement(int target, int source, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongSignedCopy(int target, int source, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongSignedConstant(long constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongSignedDelta(int target, int source, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongSignedNone(int target, long[] rLongDictionary, int[] is) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadLongSignedDefaultOptional(long constAbsent, long constDefault, int[] rbB) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadLongSignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongSignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongSignedConstantOptional(long constAbsent, long constConst, int[] rbB) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadLongSignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongSignedNoneOptional(long constAbsent, int[] rbB) {
        generator(new Exception().getStackTrace(),constAbsent);
    }

    // text methods.

    
    @Override
    protected void genReadUTF8None(int idx, int optOff, int[] rbB) {
        generator(new Exception().getStackTrace(),idx,optOff);
    }
    
    @Override
    protected void genReadUTF8TailOptional(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8DeltaOptional(int idx, int[] is) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8Delta(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIITail(int idx, int fromIdx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx,fromIdx);
    }
    
    @Override
    protected void genReadTextConstant(int constIdx, int constLen, int[] rbB, int rbMask) {
        generator(new Exception().getStackTrace(),constIdx,constLen);
    }
    
    @Override
    protected void genReadASCIIDelta(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIICopy(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8Tail(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8Copy(int idx, int optOff, int[] rbB) {
        generator(new Exception().getStackTrace(),idx,optOff);
    }
    
    @Override
    protected void genReadUTF8Default(int idx, int defLen, int optOff, int[] rbB) {
        generator(new Exception().getStackTrace(),idx,defLen,optOff);
    }
    
    @Override
    protected void genReadASCIINone(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIITailOptional(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIIDeltaOptional(int fromIdx, int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),fromIdx,idx);
    }
    
    @Override
    protected void genReadTextConstantOptional(int constInit, int constValue, int constInitLen, int constValueLen, int[] rbB) {
        generator(new Exception().getStackTrace(),constInit,constValue,constInitLen,constValueLen);
    }
    
    @Override
    protected void genReadASCIICopyOptional(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIIDefault(int idx, int defLen, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx,defLen);
    }
    
    //byte methods
    
    @Override
    protected void genReadBytesConstant(int constIdx, int constLen, int[] rbB) {
        generator(new Exception().getStackTrace(),constIdx,constLen);
    }
    
    @Override
    protected void genReadBytesConstantOptional(int constInit, int constInitLen, int constValue, int constValueLen, int[] rbB) {
        generator(new Exception().getStackTrace(),constInit,constInitLen,constValue,constValueLen);
    }
    
    @Override
    protected void genReadBytesDefault(int idx, int defLen, int optOff, int[] rbB) {
        generator(new Exception().getStackTrace(),idx,defLen,optOff);
    }
    
    @Override
    protected void genReadBytesCopy(int idx, int optOff, int[] rbB) {
        generator(new Exception().getStackTrace(),idx,optOff);
    }
    
    @Override
    protected void genReadBytesDeltaOptional(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesTailOptional(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesDelta(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesTail(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesNoneOptional(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesNone(int idx, int[] rbB) {
        generator(new Exception().getStackTrace(),idx);
    }

    // dictionary reset
    
    @Override
    protected void genReadDictionaryBytesReset(int idx, FieldReaderBytes readerBytes) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadDictionaryTextReset(int idx, TextHeap textHeap) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadDictionaryLongReset(int idx, long[] rLongDictionary, long[] rLongInit) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadDictionaryIntegerReset(int idx, int[] rIntDictionary, int[] rIntInit) {
        generator(new Exception().getStackTrace(),idx);
    }
    
}
