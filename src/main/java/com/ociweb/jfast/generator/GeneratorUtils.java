package com.ociweb.jfast.generator;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;
import com.ociweb.jfast.stream.GeneratorDriving;
import com.ociweb.jfast.stream.RingBuffers;

public class GeneratorUtils {

  //TODO: A, need histogram class with configurable slots, must support 1024 or more slots.
    //Must time the entry method and each fragment and each field.
    
    public static void generateHead(SourceTemplates templates, byte[] origCatBytes, Appendable target, String name, String base) throws IOException {
        target.append("package "+FASTClassLoader.GENERATED_PACKAGE+";\n"); //package
        target.append("\n");
        target.append(templates.imports()); //imports
        target.append("\n");
        target.append("public final class "+name+" extends "+base+" {"); //open class
        target.append("\n");
        target.append("public static byte[] catBytes = new byte[]"+(Arrays.toString(origCatBytes).replace('[', '{').replace(']', '}'))+";\n"); //static constant
        target.append("\n");
        target.append("public "+name+"() {super(new "+TemplateCatalogConfig.class.getSimpleName()+"(catBytes));}");//constructor
        target.append("\n");
    }

    public static int complexity(CharSequence seq) {
        int complexity = 0;
        int i = seq.length();
        while (--i>=0) {
            char c = seq.charAt(i);
            if ('.'==c || //deref 
                '['==c || //array ref
                '+'==c || //add
                '-'==c || //subtract
                '*'==c || //multiply
                '&'==c || //and
                '|'==c || //or
                '?'==c ) { //ternary 
                complexity++;
            }
        }
        return complexity;
    }

    public static void generateTail(Appendable target) throws IOException {
        target.append('}');
    }

    static String getSingleGroupMethod(List<String> doneScriptsParas, 
                                        List<String> caseParaDefs, 
                                        List<String> caseParaVals,
                                        int scriptPos, StringBuilder groupMethodBuilder, String caseTail, StringBuilder fieldMethodBuilder) {
        
        String paraDefs = caseParaDefs.toString().substring(1);
        paraDefs = paraDefs.substring(0, paraDefs.length()-1);
        
        String paraVals = caseParaVals.toString().substring(1);
        paraVals = paraVals.substring(0, paraVals.length()-1);
        doneScriptsParas.add(paraVals);
        
        StringBuilder signatureLine = new StringBuilder();
        signatureLine.append("private static void ")
                     .append(GeneratorData.FRAGMENT_METHOD_NAME)
                     .append(scriptPos)
                     .append("(")
                     .append(paraDefs)
                     .append(") {\n");
          
        return signatureLine.toString()+groupMethodBuilder.toString()+caseTail+fieldMethodBuilder.toString();
    }

    public static void buildEntryDispatchMethod(List<Integer> doneScripts, List<String> doneScriptsParas, Appendable builder, String entryMethodName, Class primClass) throws IOException {
    
        boolean isReader = PrimitiveReader.class==primClass;
        String primVarName = isReader ? "reader" : "writer";
        
        
        assert(doneScripts.size() == doneScriptsParas.size());
        int j = 0;
        int[] doneValues = new int[doneScripts.size()];
        String[] doneCode = new String[doneScripts.size()];
        for(Integer d:doneScripts) {
            //rbRingBuffer.buffer, rbRingBuffer.mask
            
            String methodCallArgs = doneScriptsParas.get(j)
                                    .replace("dispatch","this")
                                    .replace("rbRingBuffer","rb")
                                    .replace("rbPos","rb.addPos")
                                    .replace("rbB","rb.buffer")
                                    .replace("rbMask", "rb.mask");
            doneCode[j] = "\n\r"+
                          " rb="+RingBuffers.class.getSimpleName()+".get(ringBuffers,"+d+");\n\r"+
                          GeneratorData.FRAGMENT_METHOD_NAME+d+"("+methodCallArgs+");\n";
            doneValues[j++] = d;
        }
        BalancedSwitchGenerator bsg = new BalancedSwitchGenerator();
        builder.append("public final int "+entryMethodName+"("+primClass.getSimpleName()+" "+primVarName+") {\n");
        
        //if this is the beginning of a new template we use this special logic to pull the template id
        if (isReader) {
            builder.append("    if (activeScriptCursor<0) {\n");
            builder.append("        if (PrimitiveReader.isEOF("+primVarName+")) { \n");
            builder.append("            return -1;\n");
            builder.append("        } \n");
            builder.append("        beginMessage("+primVarName+",this);\n");
            builder.append("    }\n");
        } else {
            //TODO: A, need custom write method here.
            
        }
        
        //now that the cursor position / template id is known do normal processing
        builder.append("    int x = activeScriptCursor;\n");
        builder.append("    "+FASTRingBuffer.class.getSimpleName()+" rb;\n");
        bsg.generate("    ",builder, doneValues, doneCode);
        builder.append("    FASTRingBuffer.unBlockFragment(rb);\n");
        builder.append("    return ringBufferIdx;\n"); 
        builder.append("}\n");
    
    }

    public static void reportErrorDetails(NullPointerException npe) {
        StackTraceElement[] stackTrace = npe.getStackTrace();
        int j = 0;
        while (j<stackTrace.length) {
            //Check for programming error where the template was modified without overriding the method here.
            String className = stackTrace[j].getClassName();
            String method = stackTrace[j++].getMethodName();
            if (method.startsWith("gen") &&
                !method.startsWith("generate") &&
                !GeneratorUtils.class.getSimpleName().equals(className)) {
                System.err.println("Must override: "+className+"."+method+" to prevent running logic while generating.");
                System.exit(0);
            }
            
        }
        throw npe;
    }

    public static void generateParameters(String[] params, String[] defs, StringBuilder fieldParaValues,
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

    public  static Set<String> convertParamsToSet(String in) {
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

    /**
     * Regardless of param order determine if the child method can find all the
     * arguments it needs from the parent.
     */
    public static boolean lastMethodContainsParams(String curFieldParaValues, String lastFieldParaValues) {
        
        Set<String> paraSetParent = convertParamsToSet(lastFieldParaValues);
        Set<String> paraSetChild = convertParamsToSet(curFieldParaValues);
        return paraSetParent.containsAll(paraSetChild);
        
    }

    public static boolean validateMethodSize(String comment, int additionalComplexity) {
        if (additionalComplexity>40) {
            System.err.print("too big for inline try to make method smaller. "+additionalComplexity+"  "+comment);
        }
        return true;
    }

    public static void beginSingleGroupMethod(int scriptPos, int templateId, GeneratorData generatorData) {
        generatorData.fieldMethodBuilder.setLength(0);
        generatorData.groupMethodBuilder.setLength(0);
        generatorData.caseParaDefs.clear();
        generatorData.caseParaVals.clear();
        generatorData.scriptPos = scriptPos;
        generatorData.templateId = templateId;
        
        //each field method will start with the templateId for easy debugging later.
        generatorData.fieldPrefix = Integer.toString(templateId);
        while (generatorData.fieldPrefix.length()<4) {
            generatorData.fieldPrefix = "0"+generatorData.fieldPrefix;
        }        
        
        generatorData.fieldPrefix = "m"+generatorData.fieldPrefix;
    }

    public static String buildMethodName(GeneratorData generatorData) {
        
        generatorData.fieldMethodCount++;
        String methodName = Integer.toHexString(generatorData.fieldMethodCount);
        while (methodName.length()<3) {
            methodName = "0"+methodName;
        }
        methodName = generatorData.fieldPrefix+"_"+methodName;
        return methodName;
        
    }

    public static String buildSingleGroupMethod(int i, int fragmentStart, int limit, List<String> doneScriptsParas, GeneratorDriving scriptor, GeneratorData generatorData) {
        beginSingleGroupMethod(fragmentStart,i-1, generatorData);
        scriptor.setActiveScriptCursor(fragmentStart);
        scriptor.setActiveScriptLimit(limit); 
        try {
            scriptor.runFromCursor();//Generate the code, if any method was missed a null pointer will result.
        } catch (NullPointerException npe) {
            reportErrorDetails(npe);
        }
        return getSingleGroupMethod(doneScriptsParas, generatorData.caseParaDefs, generatorData.caseParaVals, generatorData.scriptPos, generatorData.groupMethodBuilder, generatorData.caseTail, generatorData.fieldMethodBuilder);
    }

    static String generateOpenTemplate(GeneratorData generatorData, GeneratorDriving scriptor) {
        generatorData.fieldMethodBuilder.setLength(0);
        generatorData.groupMethodBuilder.setLength(0);
        generatorData.caseParaDefs.clear();
        generatorData.caseParaVals.clear();
        
        //each field method will start with the templateId for easy debugging later.
        generatorData.fieldPrefix = "t";
        
        try {
            scriptor.setActiveScriptCursor(0);
            scriptor.runBeginMessage();
        } catch (NullPointerException npe) {
            reportErrorDetails(npe);
        }
        
        String paraDefs = generatorData.caseParaDefs.toString().substring(1);
        paraDefs = paraDefs.substring(0, paraDefs.length()-1);
        
        String paraVals = generatorData.caseParaVals.toString().substring(1);
        paraVals = paraVals.substring(0, paraVals.length()-1);
        
        StringBuilder signatureLine = new StringBuilder();
        signatureLine.append("private static void ")
                     .append("beginMessage")
                     .append("(")
                     .append(paraDefs)
                     .append(") {\n");
        
    
        return "\n"+signatureLine.toString()+generatorData.groupMethodBuilder.toString()+generatorData.caseTail+generatorData.fieldMethodBuilder.toString();
        
    }

    public static void buildGroupMethods(TemplateCatalogConfig catalog, List<Integer> doneScripts, List<String> doneScriptsParas, Appendable builder, GeneratorDriving scriptor, GeneratorData generatorData) throws IOException {
        
        //A Group may be a full message or sequence item or group.
    
        //Common method for starting new template
        builder.append(generateOpenTemplate(generatorData, scriptor));
        
        
        int[] startCursor = catalog.getTemplateStartIdx();
        int[] limitCursor = catalog.getTemplateLimitIdx();
        int i = 0;
        while (i<startCursor.length) {
            int fragmentStart = startCursor[i];
            int limit = limitCursor[i++];
            
            if (0==fragmentStart && 0==limit) {
                continue;//skip this one it was not at an entry point
            }
            
            doneScripts.add(fragmentStart);
            
            String block;
            
            block = buildSingleGroupMethod(i, fragmentStart, limit, doneScriptsParas, scriptor, generatorData);
            
            builder.append("\n");
            builder.append(block);
            
            //do additional case methods if needed.
            
            for(int seqStart:generatorData.sequenceStarts) {
                if (!doneScripts.contains(seqStart)) {
                    doneScripts.add(seqStart);
                    
                    block = buildSingleGroupMethod(i, seqStart, limit, doneScriptsParas, scriptor, generatorData);
                    
                    builder.append("\n");
                    builder.append(block);
                }
                
            }
        }
    }

    static void generator(StackTraceElement[] trace, GeneratorData generatorData, GeneratorDriving scriptor, long ... values) {
        
        String templateMethodName = trace[0].getMethodName();
        
//        if (scriptor instanceof FASTWriterInterpreterDispatch) {
//            System.err.println(templateMethodName);//TODO: A, this is missing a lot of calls.
//        }
        
        if (generatorData.usages.containsKey(templateMethodName)) {
            generatorData.usages.get(templateMethodName).incrementAndGet();
        } else {
            generatorData.usages.put(templateMethodName,new AtomicInteger(1));
        }
        
        
        String methodNameKey = " "+templateMethodName+'('; ///must include beginning and end to ensure match
        String[] paraVals = generatorData.templates.params(methodNameKey);
        String[] paraDefs = generatorData.templates.defs(methodNameKey);
        String comment = "        //"+trace[0].getMethodName()+(Arrays.toString(paraVals).replace('[','(').replace(']', ')'))+"\n";
        
        //template details to add as comments
        int token = scriptor.getActiveToken();
        int fieldId = scriptor.getActiveFieldId(); 
        String fieldName = scriptor.getActiveFieldName();     
        comment+="        //name='"+fieldName+"' id="+fieldId+" token="+TokenBuilder.tokenToString(token)+"\n";
    
        
        //replace variables with constants
        String template = generatorData.templates.template(methodNameKey);
    
    
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
            if (!generatorData.caseParaDefs.contains(paraDefs[i])) {
                generatorData.caseParaDefs.add(paraDefs[i]);
                generatorData.caseParaVals.add(paraVals[i]);
            }
            i++;
        }
    
        String methodName = buildMethodName(generatorData);
                       
        
        if (methodNameKey.contains("Length")) {
            generatorData.fieldMethodBuilder.append("private static void ").append(methodName).append("(").append(fieldParaDefs).append(") {\n");;
            //insert field operator content into method
            generatorData.fieldMethodBuilder.append(comment).append(template);
            //close field method
            generatorData.fieldMethodBuilder.append(GeneratorData.END_FIELD_METHOD);
            //add call to this method from the group method  
            generatorData.groupMethodBuilder.append("    ").append(methodName).append("(").append(fieldParaValues).append(");\n");
            generatorData.runningComplexity = 0;
            generatorData.lastFieldParaValues="_";
        } else {
            //TODO: X, if the previous para values are the same and if the method will not be too large and still in the same group.
            // back up field builder and add the new block into the existing method, no field call needs to be added to case/group
            String curFieldParaValues = fieldParaValues.toString();
            int additionalComplexity = complexity(template);
    
            assert(validateMethodSize(comment, additionalComplexity));
            
            if (lastMethodContainsParams(curFieldParaValues, generatorData.lastFieldParaValues) &&
                additionalComplexity+generatorData.runningComplexity<=GeneratorData.COMPLEXITY_LIMITY_PER_METHOD && 
                generatorData.fieldMethodBuilder.length()>0) {
                //this field has the same parameters as the  previous and
                //adding this complexity is under the limit and
                //previous method was appended onto builder
                //so combine this.
                
                //strip off the method close so we can tack some more work in it.
                assert(generatorData.fieldMethodBuilder.toString().endsWith(GeneratorData.END_FIELD_METHOD));
                generatorData.fieldMethodBuilder.setLength(generatorData.fieldMethodBuilder.length()-GeneratorData.END_FIELD_METHOD.length());
                                
                //insert field operator content into method
                generatorData.fieldMethodBuilder.append(comment).append(template);
                //close field method
                generatorData.fieldMethodBuilder.append(GeneratorData.END_FIELD_METHOD);
                
                generatorData.runningComplexity += additionalComplexity;
                
                //Do not change lastFieldParaValues
                
            } else {
                
                //method signature line
                generatorData.fieldMethodBuilder.append("private static void ").append(methodName).append("(").append(fieldParaDefs).append(") {\n");
          
                //insert field operator content into method
                generatorData.fieldMethodBuilder.append(comment).append(template);
                //close field method
                generatorData.fieldMethodBuilder.append(GeneratorData.END_FIELD_METHOD);
                
                //add call to this method from the group method
                generatorData.groupMethodBuilder.append("    ").append(methodName).append("(").append(curFieldParaValues).append(");\n");
    
                generatorData.runningComplexity = additionalComplexity;
                generatorData.lastFieldParaValues = curFieldParaValues;
            }
        }
    }

}
