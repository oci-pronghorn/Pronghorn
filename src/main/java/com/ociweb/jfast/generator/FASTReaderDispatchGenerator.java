package com.ociweb.jfast.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;

public class FASTReaderDispatchGenerator extends FASTReaderInterpreterDispatch {

    // TODO: X, look into core affinity
    // TODO: C, code does not support final in signatures, this would be nice to have
    
    private static final String END_FIELD_METHOD = "};\n";
    
    //A fragment is the smallest unit that can be passed to the caller. It is never larger than a group but may often be the same size as one.
    private static final String FRAGMENT_METHOD_NAME = "fragment";    
    
    private static final int COMPLEXITY_LIMITY_PER_METHOD = 64;//32;//128;//NOTE: we may want to make this smaller in the production release.
    private static final String ENTRY_METHOD_NAME = "decode";
    
    SourceTemplates templates;
    
    StringBuilder fieldMethodBuilder;
    StringBuilder groupMethodBuilder;
    List<String> caseParaDefs = new ArrayList<String>(); 
    List<String> caseParaVals = new ArrayList<String>(); 
    int scriptPos;
    int templateId;
    
    String fieldPrefix;
    int fieldMethodCount;
    
    String caseTail = "}\n";
    Set<Integer> sequenceStarts = new HashSet<Integer>();
    byte[] origCatBytes;
    
    
    public FASTReaderDispatchGenerator(byte[] catBytes) {
        super(new TemplateCatalog(catBytes));
        
        origCatBytes = catBytes;
        templates = new SourceTemplates();
        fieldMethodBuilder = new StringBuilder();
        groupMethodBuilder = new StringBuilder();
        fieldMethodCount = 0;
    }
    
    
    //This generator allows for refactoring of the NAME of these methods and the code generation will remain intact.
    
    private String getSingleGroupMethod(List<String> doneScriptsParas) {
        
        String paraDefs = caseParaDefs.toString().substring(1);
        paraDefs = paraDefs.substring(0, paraDefs.length()-1);
        
        String paraVals = caseParaVals.toString().substring(1);
        paraVals = paraVals.substring(0, paraVals.length()-1);
        doneScriptsParas.add(paraVals);
        
        StringBuilder signatureLine = new StringBuilder();
        signatureLine.append("private static void ")
                     .append(FRAGMENT_METHOD_NAME)
                     .append(scriptPos)
                     .append("(")
                     .append(paraDefs)
                     .append(") {\n");
        
        signatureLine.append("//script ").append(activeScriptCursor).append('-').append(activeScriptLimit)
                    .append(" id:").append(fieldIdScript[activeScriptCursor]).append("\n");

       // groupMethodBuilder.append("    System.out.println(\"called "+FRAGMENT_METHOD_NAME+scriptPos+"  \");\n");
        
        return signatureLine.toString()+groupMethodBuilder.toString()+caseTail+fieldMethodBuilder.toString();
    }
    
    private void beginSingleGroupMethod(int scriptPos, int templateId) {
        fieldMethodBuilder.setLength(0);
        groupMethodBuilder.setLength(0);
        caseParaDefs.clear();
        caseParaVals.clear();
        this.scriptPos = scriptPos;
        this.templateId = templateId;
        
        //each field method will start with the templateId for easy debugging later.
        fieldPrefix = Integer.toString(templateId);
        while (fieldPrefix.length()<4) {
            fieldPrefix = "0"+fieldPrefix;
        }
        
        
        fieldPrefix = "m"+fieldPrefix;
    }
    
    public Set<Integer> getSequenceStarts() {
        return sequenceStarts;
    }
    
    private void generator(StackTraceElement[] trace, long ... values) {
        
        String methodNameKey = " "+trace[0].getMethodName()+'('; ///must include beginning and end to ensure match
        String[] paraVals = templates.params(methodNameKey);
        String[] paraDefs = templates.defs(methodNameKey);
        String comment = "        //"+trace[0].getMethodName()+(Arrays.toString(paraVals).replace('[','(').replace(']', ')'))+"\n";
        
        //template details to add as comments
        int token = fullScript[activeScriptCursor];
        int fieldId = fieldIdScript[activeScriptCursor];
        String fieldName = fieldNameScript[activeScriptCursor];        
        comment+="        //name='"+fieldName+"' id="+fieldId+" token="+TokenBuilder.tokenToString(token)+"\n";

        
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
            
            template = template.replace(paraVals[i],strData+"/*"+paraVals[i]+"="+Long.toString(data[i])+"*/");
        }
        
        
        StringBuilder fieldParaValues = new StringBuilder();
        StringBuilder fieldParaDefs = new StringBuilder();
        generateParameters(paraVals, paraDefs, fieldParaValues, fieldParaDefs, data.length);
        
        //accumulate new paras for case method.
        i = data.length;
        while (i<paraVals.length) {
            if (!caseParaDefs.contains(paraDefs[i])) {
                caseParaDefs.add(paraDefs[i]);
                caseParaVals.add(paraVals[i]);
            }
            i++;
        }

        String methodName = buildMethodName();
                       
        
        if (methodNameKey.contains("Length")) {
            fieldMethodBuilder.append("private static int ").append(methodName).append("(").append(fieldParaDefs).append(") {\n");;
            //insert field operator content into method
            fieldMethodBuilder.append(comment).append(template);
            //close field method
            fieldMethodBuilder.append(END_FIELD_METHOD);
            //add call to this method from the group method  TODO: A, inline assignemnt so it need not be here.
            groupMethodBuilder.append("    dispatch.activeScriptCursor=").append(methodName).append("(").append(fieldParaValues).append(");\n");
            runningComplexity = 0;
            lastFieldParaValues="_";
        } else {
            //TODO: X, if the previous para values are the same and if the method will not be too large and still in the same group.
            // back up field builder and add the new block into the existing method, no field call needs to be added to case/group
            String curFieldParaValues = fieldParaValues.toString();
            int additionalComplexity = GeneratorUtils.complexity(template);
 
            assert(validateMethodSize(comment, additionalComplexity));
            
            if (lastMethodContainsParams(curFieldParaValues) &&
                additionalComplexity+runningComplexity<=COMPLEXITY_LIMITY_PER_METHOD && 
                fieldMethodBuilder.length()>0) {
                //this field has the same parameters as the  previous and
                //adding this complexity is under the limit and
                //previous method was appended onto builder
                //so combine this.
                
                //strip off the method close so we can tack some more work in it.
                assert(fieldMethodBuilder.toString().endsWith(END_FIELD_METHOD));
                fieldMethodBuilder.setLength(fieldMethodBuilder.length()-END_FIELD_METHOD.length());
                                
                //insert field operator content into method
                fieldMethodBuilder.append(comment).append(template);
                //close field method
                fieldMethodBuilder.append(END_FIELD_METHOD);
                
                runningComplexity += additionalComplexity;
                
                //Do not change lastFieldParaValues
                
            } else {
                
                //method signature line
                fieldMethodBuilder.append("private static void ").append(methodName).append("(").append(fieldParaDefs).append(") {\n");
          
                //insert field operator content into method
                fieldMethodBuilder.append(comment).append(template);
                //close field method
                fieldMethodBuilder.append(END_FIELD_METHOD);
                
                //add call to this method from the group method
                groupMethodBuilder.append("    ").append(methodName).append("(").append(curFieldParaValues).append(");\n");

                runningComplexity = additionalComplexity;
                lastFieldParaValues = curFieldParaValues;
            }
        }
    }


    private boolean validateMethodSize(String comment, int additionalComplexity) {
        if (additionalComplexity>30) {
            System.err.print("too big for inline "+additionalComplexity+"  "+comment);
        }
        return true;
    }


    /**
     * Regardless of param order determine if the child method can find all the
     * arguments it needs from the parent.
     * 
     * @param curFieldParaValues
     * @return
     */
    private boolean lastMethodContainsParams(String curFieldParaValues) {
        
        Set<String> paraSetParent = convertParamsToSet(lastFieldParaValues);
        Set<String> paraSetChild = convertParamsToSet(curFieldParaValues);
        return paraSetParent.containsAll(paraSetChild);
        
    }


    private Set<String> convertParamsToSet(String in) {
        String[] paras = in.split(",");
        Set<String> paraSet = new HashSet<String>();
        int i = paras.length;
        while (--i>=0) {
            String temp = paras[i].trim();
            if (temp.length()>=0) {
                paraSet.add(temp);
            }       
        }
        return paraSet;
    }



    private String buildMethodName() {
        
        fieldMethodCount++;
        String methodName = Integer.toHexString(fieldMethodCount);
        while (methodName.length()<3) {
            methodName = "0"+methodName;
        }
        methodName = fieldPrefix+"_"+methodName;
        return methodName;
        
    }
    
    int    runningComplexity = 0;
    String lastFieldParaValues="_";

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
    
    private void generateGroupMethods(TemplateCatalog catalog, List<Integer> doneScripts, List<String> doneScriptsParas, Appendable builder) throws IOException {
        
        //A Group may be a full message or sequence item or group.
        
        int[] startCursor = catalog.templateStartIdx;
        int[] limitCursor = catalog.templateLimitIdx;
        int i = 0;
        while (i<startCursor.length) {
            int cursor = startCursor[i];
            int limit = limitCursor[i++];
            
            if (0==cursor && 0==limit) {
                continue;//skip this one it was not at an entry point
            }
            
            doneScripts.add(cursor);
            
            String block;
            
            block = generateSingleGroupMethod(i, cursor, limit, doneScriptsParas);
            
            builder.append("\n");
            builder.append(block);
            
            //do additional case methods if needed.
            
            for(int seqStart:getSequenceStarts()) {
                if (!doneScripts.contains(seqStart)) {
                    doneScripts.add(seqStart);
                    
                    block = generateSingleGroupMethod(i, seqStart, limit, doneScriptsParas);
                    
                    builder.append("\n");
                    builder.append(block);
                }
                
            }
        }
    }
    private String generateSingleGroupMethod(int i, int cursor, int limit, List<String> doneScriptsParas) {
        beginSingleGroupMethod(cursor,i-1);
        activeScriptCursor = cursor;
        activeScriptLimit = limit;
        decode(null);//Generate the code, if any method was missed a null pointer will result.
        return getSingleGroupMethod(doneScriptsParas);
    }
    
    private void generateEntryDispatchMethod(List<Integer> doneScripts, List<String> doneScriptsParas, Appendable builder) throws IOException {
        assert(doneScripts.size() == doneScriptsParas.size());
        int j = 0;
        int[] doneValues = new int[doneScripts.size()];
        String[] doneCode = new String[doneScripts.size()];
        for(Integer d:doneScripts) {
            //rbRingBuffer.buffer, rbRingBuffer.mask
            
            //TODO: A, custom ring buffer per calls.
            String methodCallArgs = doneScriptsParas.get(j)
                                    .replace("dispatch","this")
                                    .replace("rbRingBuffer","ringBuffer()")
                                    .replace("rbB","ringBuffer().buffer")
                                    .replace("rbMask", "ringBuffer().mask");

            int token = fullScript[activeScriptCursor];
            doneCode[j] = "assert (gatherReadData(reader, activeScriptCursor,"+token+"));\n\r"+FRAGMENT_METHOD_NAME+d+"("+methodCallArgs+");\n";
            doneValues[j++] = d;
        }
        BalancedSwitchGenerator bsg = new BalancedSwitchGenerator();
        builder.append("public final boolean "+ENTRY_METHOD_NAME+"(PrimitiveReader reader) {\n");
        builder.append("    int x = activeScriptCursor;\n");
        bsg.generate("    ",builder, doneValues, doneCode);
        builder.append("    return sequenceCountStackHead>=0;\n"); 
        builder.append("}\n");

    }

    
    
    public <T extends Appendable> T generateFullReaderSource(T target) throws IOException {
        List<Integer> doneScripts = new ArrayList<Integer>();
        List<String> doneScriptsParas = new ArrayList<String>();
        
        GeneratorUtils.generateHead(templates, origCatBytes, target, FASTClassLoader.SIMPLE_READER_NAME, FASTDecoder.class.getSimpleName());
        generateGroupMethods(new TemplateCatalog(origCatBytes),doneScripts,doneScriptsParas,target);
        generateEntryDispatchMethod(doneScripts,doneScriptsParas,target);
        GeneratorUtils.generateTail(target);
        
        return target;
    }

    
    //TODO: C, Add API for getting class/source and for setting class file.
    
   

    
    @Override
    protected void genReadSequenceClose(int backvalue, int topCursorPos, FASTDecoder dispatch) {
        generator(new Exception().getStackTrace(),backvalue, topCursorPos);
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
    protected int genReadLengthDefault(int constDefault,  int jumpToTarget, int jumpToNext, int[] rbB, PrimitiveReader reader, int rbMask, FASTRingBuffer rbRingBuffer, FASTDecoder dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),constDefault,jumpToTarget,jumpToNext);
        return jumpToNext;
    }

    @Override
    protected int genReadLengthIncrement(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTDecoder dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target,source,jumpToTarget,jumpToNext);
        return jumpToNext;
    }

    @Override
    protected int genReadLengthCopy(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTDecoder dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target,source,jumpToTarget,jumpToNext);
        return jumpToNext;
    }

    @Override
    protected int genReadLengthConstant(int constDefault, int jumpToTarget, int jumpToNext, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, FASTDecoder dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),constDefault,jumpToTarget,jumpToNext);
        return jumpToNext;
    }

    @Override
    protected int genReadLengthDelta(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTDecoder dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target,source,jumpToTarget,jumpToNext);
        return jumpToNext;
    }

    @Override
    protected int genReadLength(int target,  int jumpToTarget, int jumpToNext, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, int[] rIntDictionary, PrimitiveReader reader, FASTDecoder dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target, jumpToTarget,jumpToNext);
        return jumpToNext;
    }
    
    // copy methods
    
    @Override
    protected void genReadCopyText(int source, int target, TextHeap textHeap) {
        generator(new Exception().getStackTrace(),source,target);
    }

    @Override
    protected void genReadCopyBytes(int source, int target, ByteHeap byteHeap) {
        generator(new Exception().getStackTrace(),source,target);
    }
    
    // int methods

    @Override
    protected void genReadIntegerUnsignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }

    @Override
    protected void genReadIntegerUnsignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadIntegerUnsignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent);
    }
    
    @Override
    protected void genReadIntegerUnsignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerUnsignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerUnsignedConstant(int constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerUnsignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerUnsigned(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadIntegerSignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerSignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerConstant(int constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadIntegerSignedNone(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadIntegerSignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadIntegerSignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadIntegerSignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadIntegerSignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent);
    }
    
    ///
    
    @Override
    protected void genReadExponentDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadExponentIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadExponentCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadExponentConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadExponentDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadExponentOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent);
    }
    

    // long methods
    
    @Override
    protected void genReadLongUnsignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedIncrement(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongUnsignedCopy(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongConstant(long constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedDelta(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongUnsignedNone(int target, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadLongUnsignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadLongUnsignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }

    @Override
    protected void genReadLongUnsignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongUnsignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadLongUnsignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongUnsignedOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent);
    }
    
    @Override
    protected void genReadLongSignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongSignedIncrement(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongSignedCopy(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongSignedConstant(long constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constDefault);
    }
    
    @Override
    protected void genReadLongSignedDelta(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source);
    }
    
    @Override
    protected void genReadLongSignedNone(int target, long[] rLongDictionary, int[] is, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target);
    }
    
    @Override
    protected void genReadLongSignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constDefault);
    }
    
    @Override
    protected void genReadLongSignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongSignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongSignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent,constConst);
    }
    
    @Override
    protected void genReadLongSignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),target,source,constAbsent);
    }
    
    @Override
    protected void genReadLongSignedNoneOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constAbsent);
    }

    // text methods.

    
    @Override
    protected void genReadUTF8None(int idx, int optOff, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx,optOff);
    }
    
    @Override
    protected void genReadUTF8TailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8DeltaOptional(int idx, int[] is, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8Delta(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIITail(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadTextConstant(int constIdx, int constLen, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constIdx,constLen);
    }
    
    @Override
    protected void genReadASCIIDelta(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIICopy(int idx, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8Tail(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadUTF8Copy(int idx, int optOff, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx,optOff);
    }
    
    @Override
    protected void genReadUTF8Default(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx,defIdx, defLen,optOff);
    }
    
    @Override
    protected void genReadASCIINone(int idx, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIITailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIIDeltaOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadTextConstantOptional(int constInit, int constValue, int constInitLen, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constInit,constValue,constInitLen,constValueLen);
    }
    
    @Override
    protected void genReadASCIICopyOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadASCIIDefault(int idx, int defIdx, int defLen, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx,defIdx,defLen);
    }
    
    //byte methods
    
    @Override
    protected void genReadBytesConstant(int constIdx, int constLen, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constIdx,constLen);
    }
    
    @Override
    protected void genReadBytesConstantOptional(int constInit, int constInitLen, int constValue, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),constInit,constInitLen,constValue,constValueLen);
    }
    
    @Override
    protected void genReadBytesDefault(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx,defIdx, defLen,optOff);
    }
    
    @Override
    protected void genReadBytesCopy(int idx, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx,optOff);
    }
    
    @Override
    protected void genReadBytesDeltaOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesTailOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesDelta(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesTail(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesNoneOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesNone(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        generator(new Exception().getStackTrace(),idx);
    }

    // dictionary reset
    
    @Override
    protected void genReadDictionaryBytesReset(int idx, ByteHeap byteHeap) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadDictionaryTextReset(int idx, TextHeap textHeap) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadDictionaryLongReset(int idx, long resetConst, long[] rLongDictionary) {
        generator(new Exception().getStackTrace(),idx, resetConst);
    }
    
    @Override
    protected void genReadDictionaryIntegerReset(int idx, int resetConst, int[] rIntDictionary) {
        generator(new Exception().getStackTrace(),idx, resetConst);
    }

    
}
