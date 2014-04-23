package com.ociweb.jfast.stream;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
        //replace ring buffer position increment
        template = template.replace("spclPosInc()", "queue.addPos++")+"\n";
        
        fieldCount++;
        String field = Integer.toHexString(fieldCount);
        while (field.length()<3) {
            field = "0"+field;
        }
        field = fieldPrefix+"_"+field;
        
        if (methodNameKey.contains("Length")) {
            fieldBuilder.append("private boolean ");
            caseBuilder.append("    if (").append(field).append("()) {return;};\n");
        } else {
            fieldBuilder.append("private void ");
            caseBuilder.append("    ").append(field).append("();\n");
        }
        fieldBuilder.append(field).append("() {\n").append(comment).append(template).append("};\n");
        
        
    }
    
//    @Override
//    protected int sequenceJump(int length, int cursor) {
//        sequenceStarts.add(cursor+1);
//        //force sequence to NOT be take so we can generate the block for the sequence specifically
//        return super.sequenceJump(0, cursor);
//    }
    
    @Override
    protected void genReadSequenceClose(int backvalue) {
        generator(new Exception().getStackTrace(),backvalue);
    }
    
    @Override
    protected void genReadGroupPMapOpen() {
        generator(new Exception().getStackTrace());
    }
    
    @Override
    protected void genReadGroupClose() {
        generator(new Exception().getStackTrace());
    }
    
    
    // length methods
    
    @Override
    protected boolean genReadLengthDefault(int constDefault,  int jumpToTarget) {
        generator(new Exception().getStackTrace(),constDefault,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthIncrement(int target, int source,  int jumpToTarget, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthCopy(int target, int source,  int jumpToTarget, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthConstant(int constDefault, int jumpToTarget) {
        generator(new Exception().getStackTrace(),constDefault,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthDelta(int target, int source,  int jumpToTarget, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLength(int target,  int jumpToTarget) {
        generator(new Exception().getStackTrace(),target, jumpToTarget);
        return true;
    }
    
    // int methods

    @Override
    protected void genReadIntegerUnsignedDefaultOptional(int constAbsent, int constDefault) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }

    @Override
    protected void genReadIntegerUnsignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedConstantOptional(int constAbsent, int constConst) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadIntegerUnsignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedOptional(int constAbsent) {
        generator(new Exception().getStackTrace(),constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedDefault(int constDefault) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrement(int target, int source, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerUnsignedCopy(int target, int source, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerUnsignedConstant(int constDefault) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedDelta(int target, int source, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerUnsigned(int target) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadIntegerSignedDefault(int constDefault) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedIncrement(int target, int source, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerSignedCopy(int target, int source, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerConstant(int constDefault) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedDelta(int target, int source, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerSignedNone(int target) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadIntegerSignedDefaultOptional(int constAbsent, int constDefault) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedConstantOptional(int constAbsent, int constConst) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadIntegerSignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedOptional(int constAbsent) {
        generator(new Exception().getStackTrace(),constAbsent);
    }

    // long methods
    
    @Override
    protected void genReadLongUnsignedDefault(long constDefault) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedIncrement(int target, int source, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongUnsignedCopy(int target, int source, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongConstant(long constDefault) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedDelta(int target, int source, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongUnsignedNone(int target, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadLongUnsignedDefaultOptional(long constAbsent, long constDefault) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }

    @Override
    protected void genReadLongUnsignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongUnsignedConstantOptional(long constAbsent, long constConst) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadLongUnsignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongUnsignedOptional(long constAbsent) {
        generator(new Exception().getStackTrace(),constAbsent);
    }
    
    @Override
    protected void genReadLongSignedDefault(long constDefault) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongSignedIncrement(int target, int source, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongSignedCopy(int target, int source, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongSignedConstant(long constDefault) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongSignedDelta(int target, int source, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongSignedNone(int target, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadLongSignedDefaultOptional(long constAbsent, long constDefault) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadLongSignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongSignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongSignedConstantOptional(long constAbsent, long constConst) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadLongSignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongSignedNoneOptional(long constAbsent) {
        generator(new Exception().getStackTrace(),constAbsent);
    }

    // text methods.

    
    @Override
    protected void genReadUTF8None(int idx, int optOff) {
        generator(new Exception().getStackTrace(),idx,optOff);
    }
    
    @Override
    protected void genReadUTF8TailOptional(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8DeltaOptional(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8Delta(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIITail(int idx, int fromIdx) {
        generator(new Exception().getStackTrace(),idx,fromIdx);
    }
    
    @Override
    protected void genReadTextConstant(int constIdx, int constLen) {
        generator(new Exception().getStackTrace(),constIdx,constLen);
    }
    
    @Override
    protected void genReadASCIIDelta(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIICopy(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8Tail(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8Copy(int idx, int optOff) {
        generator(new Exception().getStackTrace(),idx,optOff);
    }
    
    @Override
    protected void genReadUTF8Default(int idx, int defLen, int optOff) {
        generator(new Exception().getStackTrace(),idx,defLen,optOff);
    }
    
    @Override
    protected void genReadASCIINone(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIITailOptional(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIIDeltaOptional(int fromIdx, int idx) {
        generator(new Exception().getStackTrace(),fromIdx,idx);
    }
    
    @Override
    protected void genReadTextConstantOptional(int constInit, int constValue, int constInitLen, int constValueLen) {
        generator(new Exception().getStackTrace(),constInit,constValue,constInitLen,constValueLen);
    }
    
    @Override
    protected void genReadASCIICopyOptional(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIIDefault(int idx, int defLen) {
        generator(new Exception().getStackTrace(),idx,defLen);
    }
    
    //byte methods
    
    @Override
    protected void genReadBytesConstant(int constIdx, int constLen) {
        generator(new Exception().getStackTrace(),constIdx,constLen);
    }
    
    @Override
    protected void genReadBytesConstantOptional(int constInit, int constInitLen, int constValue, int constValueLen) {
        generator(new Exception().getStackTrace(),constInit,constInitLen,constValue,constValueLen);
    }
    
    @Override
    protected void genReadBytesDefault(int idx, int defLen, int optOff) {
        generator(new Exception().getStackTrace(),idx,defLen,optOff);
    }
    
    @Override
    protected void genReadBytesCopy(int idx, int optOff) {
        generator(new Exception().getStackTrace(),idx,optOff);
    }
    
    @Override
    protected void genReadBytesDeltaOptional(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesTailOptional(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesDelta(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesTail(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesNoneOptional(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesNone(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }

    // dictionary reset
    
    @Override
    protected void genReadDictionaryBytesReset(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadDictionaryTextReset(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadDictionaryLongReset(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadDictionaryIntegerReset(int idx) {
        generator(new Exception().getStackTrace(),idx);
    }
    
}
