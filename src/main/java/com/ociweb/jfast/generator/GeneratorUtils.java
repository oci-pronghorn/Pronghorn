package com.ociweb.jfast.generator;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.GeneratorDriving;
import com.ociweb.jfast.stream.RingBuffers;

public class GeneratorUtils {
    //TODO: D, schema is flexable and recorded with the data stream.
    
    static final boolean REMOVE_ARRAY = false; //TODO: B, not working for writer. still testing this idea, must decide after writer is finished 
    static final boolean ADD_COMMENTS = true;
    static final int COMPLEXITY_LIMITY_PER_METHOD = 30;//28;//10050;//22;//18 25;
    static final boolean OPTIMIZE_PMAP_READ_WRITE = true;
    
    
    //TODO: C, confirm this is a good or bad feature. leave true until we decide on this performance idea.
    public final static boolean WRITE_CONST = true; 

    
    public static void generateHead(GeneratorData generatorData, Appendable target, String name, String base) throws IOException {

        target.append("package "+FASTClassLoader.GENERATED_PACKAGE+";\n"); //package
        target.append("\n");
        target.append(generatorData.templates.imports()); //imports
        target.append("\n");
        target.append("public final class "+name+" extends "+base+" {"); //open class
        target.append("\n");        
        target.append("public static byte[] catBytes = new byte[]"+(Arrays.toString(generatorData.origCatBytes).replace('[', '{').replace(']', '}'))+";\n"); //static constant
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

    public static void generateTail(GeneratorData generatorData, Appendable target) throws IOException {
        //dictionary
        target.append(generatorData.dictionaryBuilderInt);
        target.append(generatorData.dictionaryBuilderLong);
        
        target.append('}');
    }

    static String getSingleFragmentMethod(List<String> doneScriptsParas, 
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
    
    
    //Called only once for generating full source file
    public static void buildEntryDispatchMethod(int preambleLength,
                                                List<Integer> doneScripts, List<String> doneScriptsParas, 
                                                Appendable builder, String entryMethodName,
                                                Class primClass, RingBuffers ringBuffers) throws IOException {
    
        boolean isReader = PrimitiveReader.class==primClass;
        String primVarName = isReader ? "reader" : "writer";
        
        
        
        assert(doneScripts.size() == doneScriptsParas.size());
        int j = 0;
        int m = 0;
        
        //Removes all the duplicates
        Set<Integer> uniqueTotal = new HashSet<Integer>();
        uniqueTotal.addAll(doneScripts);
                
        int[] doneValues = new int[uniqueTotal.size()];
        String[] doneCode = new String[uniqueTotal.size()];
        for(Integer cursorPos:doneScripts) {
        	
            String methodCallArgs = doneScriptsParas.get(m++)
                                    .replace("dispatch","this")
                                    .replace("rbRingBuffer","rb")
                                    .replace("byteBuffer", "rb.byteBuffer")
                                    .replace("byteMask", "rb.byteMask")                                    
                                    .replace("rbB","rb.buffer")
                                    .replace("rbMask", "rb.mask");
            
            if (isReader) {
                methodCallArgs = methodCallArgs.replace("rbPos","rb.workingHeadPos"); 
            } else {
                methodCallArgs = methodCallArgs.replace("rbPos","rb.workingTailPos"); 
            }
            
            int k = j;
            boolean found = false;
            while (--k >= 0) {
            	found |= (doneValues[k]==cursorPos.intValue());
            }
            if (!found) {
	            doneCode[j] = "\n\r"+
	                          " "+ //TODO: B, Clean up this is very messy
	                              (isReader?
	                               " rb="+RingBuffers.class.getSimpleName()+".get(ringBuffers,"+cursorPos+");\n\r":
	                                   "\n\r");
	                    
	            //exit if the ring buffer is full          
	            if (isReader) {
	                
	                FASTRingBuffer thisRingBuffer = RingBuffers.get(ringBuffers,cursorPos);
	                int fragmentSize = thisRingBuffer.from.fragDataSize[cursorPos]+ thisRingBuffer.from.templateOffset + 1;
	                
	            doneCode[j] += 
	                   " long neededTailStop = rb.workingHeadPos.value - "+(thisRingBuffer.maxSize-fragmentSize)+";\n\r"+ 
	                   
	                   " if (rb.consumerData.tailCache < neededTailStop && ((rb.consumerData.tailCache=rb.tailPos.longValue()) < neededTailStop) ) {\n\r"+  
	                   "       return 0;//nothing read\n\r" +
	                   " }\n\r";
	            } 
	                                                        	
            	doneCode[j] += GeneratorData.FRAGMENT_METHOD_NAME+cursorPos+"("+methodCallArgs+");\n\r";            	
            	doneValues[j++] = cursorPos;
            	
            }
            
            
        }
        BalancedSwitchGenerator bsg = new BalancedSwitchGenerator();
      

        
        
        //if this is the beginning of a new template we use this special logic to pull the template id
        if (isReader) {
            builder.append("public final int "+entryMethodName+"("+primClass.getSimpleName()+" "+primVarName+") {\n");
            builder.append("    if (activeScriptCursor<0) {\n");
            builder.append("        if (PrimitiveReader.isEOF("+primVarName+")) { \n");
            builder.append("            return -1;//end of file\n");
            builder.append("        } \n");
            builder.append("        beginMessage("+primVarName+",this);\n");
            builder.append("    }\n");
        } else {
            builder.append("public final void "+entryMethodName+"("+primClass.getSimpleName()+" "+primVarName+", FASTRingBuffer rb) {\n"); 
            
            builder.append("fieldPos = 0;\n");
            builder.append("\n");
            builder.append("setActiveScriptCursor(rb.consumerData.cursor);\n");        
            
            //TODO: X, optimization to remove this conditional. if we only have 1 fragment it is always a new message
            
            builder.append("if (rb.consumerData.isNewMessage()) {\n");                
            
            if (preambleLength==0) {
                builder.append("    beginMessage(this);\n");
            } else {
                builder.append("    beginMessage(writer, rb.buffer, rb.mask, rb.workingTailPos, this);\n");
            }
            
            builder.append("}\n"); 

        }
        
        //now that the cursor position / template id is known do normal processing
        builder.append("    int x = activeScriptCursor;\n");
        if (isReader) {
            builder.append("    "+FASTRingBuffer.class.getSimpleName()+" rb;\n");
        }
        
              
        bsg.generate("    ",builder, doneValues, doneCode);
        if (isReader) {
            builder.append("    FASTRingBuffer.publishWrites(rb);\n");
            builder.append("    return 1;//read a fragment\n"); 
        } 
        builder.append("}\n");
    
    }

    /**
     * Reports back to the developer that one of the template methods were not captured for generation.
     * Without this the developer would only see a non-descript null pointer exception instead of the true cause of the problem.
     * @param npe
     */
    private static void reportErrorDetails(NullPointerException npe) {
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
            StringBuilder fieldParaDefs, int x, String dispatchType) {
        /////////////
        ///generate params to be passed in to the method
        ///generate the param definitions in signature of each method
        ///these are the left over params from the gen method after removing values
        ///////////////
        while (x<params.length) {
            if (!REMOVE_ARRAY | 
                    (!params[x].equals("dispatch")  && 
                     !params[x].equals("rIntDictionary")  && 
                     !params[x].equals("rLongDictionary"))   ) {
                
                fieldParaValues.append(params[x]).append(',');
                fieldParaDefs.append(defs[x]).append(',');
                
                
            } else {
                if (fieldParaValues.indexOf("dispatch,")<0) {
                    fieldParaValues.append("dispatch,");
                    fieldParaDefs.append(dispatchType+" dispatch,");
                }
            }
            
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

    public static void beginSingleFragmentMethod(int scriptPos, int templateId, GeneratorData generatorData) {
        generatorData.fieldMethodBuilder.setLength(0);
        generatorData.groupMethodBuilder.setLength(0);
        generatorData.caseParaDefs.clear();
        generatorData.caseParaVals.clear();
        generatorData.scriptPos = scriptPos;
        generatorData.templateId = templateId;

        //for update of the reader
        generatorData.readerPmapBit = 6;
        
        //for update of the writer
        generatorData.writerPmapBit0 = 6;
        generatorData.writerPmapBit1 = 6;
        
        
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

    public static String buildSingleFragmentMethod(int i, int fragmentStart, int limit, List<String> doneScriptsParas, GeneratorDriving scriptor, GeneratorData generatorData) {
        beginSingleFragmentMethod(fragmentStart,i-1, generatorData);
        scriptor.setActiveScriptCursor(fragmentStart);
        scriptor.setActiveScriptLimit(limit); 
        try {
            scriptor.runFromCursor();//Generate the code, if any method was missed a null pointer will result.
            //
            //record the 'next' cursor index in case this message has stopped early at the end of a fragment.
            //this value is stored as a potential fragment start to ensure every entry point is covered.
            if (scriptor.getActiveScriptCursor()+1<scriptor.scriptLength()) {
                generatorData.sequenceStarts.add(scriptor.getActiveScriptCursor()+1);
            }
            
        } catch (NullPointerException npe) {
            reportErrorDetails(npe);
        }
        return getSingleFragmentMethod(doneScriptsParas, generatorData.caseParaDefs, generatorData.caseParaVals, 
                                    generatorData.scriptPos, generatorData.groupMethodBuilder, generatorData.caseTail, generatorData.fieldMethodBuilder);
    }

    static String generateOpenTemplate(GeneratorData generatorData, GeneratorDriving scriptor) {
        generatorData.fieldMethodBuilder.setLength(0);
        generatorData.groupMethodBuilder.setLength(0);
        generatorData.caseParaDefs.clear();
        generatorData.caseParaVals.clear();
        
        //each field method will start with the templateId for easy debugging later.
        generatorData.fieldPrefix = "t";
        
        try {//this is done only once to create the beginMessage method that will be called each time a new message starts.
            scriptor.setActiveScriptCursor(0);//just to prevent out of bounds error this has nothing to do with the cursor postion.
            scriptor.runBeginMessage();
        } catch (NullPointerException npe) {
            reportErrorDetails(npe);
        }
        
        String paraDefs = generatorData.caseParaDefs.toString().substring(1);
        paraDefs = paraDefs.substring(0, paraDefs.length()-1);
        
        //Must add argument to ensure dispatch is available inside the method.
        if (generatorData.dispatchType.contains("Writer")) {
            if (!paraDefs.contains("FASTEncoder dispatch")) {
                if (paraDefs.length()>0) {
                    paraDefs = paraDefs+",";
                }                
                paraDefs = paraDefs+"FASTEncoder dispatch";
            }
        }
        
        String paraVals = generatorData.caseParaVals.toString().substring(1);
        paraVals = paraVals.substring(0, paraVals.length()-1);
        
        StringBuilder signatureLine = new StringBuilder();
        signatureLine.append("\n")
                     .append("private static void ")
                     .append("beginMessage")
                     .append("(")
                     .append(paraDefs)
                     .append(") {\n")
                     .append(generatorData.groupMethodBuilder);
     
        
        //above the method was checked to ensure it has the dispatch parameter
        if (generatorData.dispatchType.contains("Writer")) {
            signatureLine.append("dispatch.fieldPos++;\n");
        }
        
        
        return  signatureLine.toString()+ 
                generatorData.caseTail+
                generatorData.fieldMethodBuilder.toString();
        
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
            
            block = buildSingleFragmentMethod(i, fragmentStart, limit, doneScriptsParas, scriptor, generatorData);
            
            builder.append("\n");
            builder.append(block);
            
            //do additional case methods if needed.
            
            //generatorData.sequenceStarts.
            
            
            //Must make copy because we will be modifying this set inside the loop
            Set<Integer> clonedStarts = new HashSet<Integer>(generatorData.sequenceStarts);
           // System.err.println("seq starts:"+generatorData.sequenceStarts+" "+generatorData.dispatchType);
            
            for(int seqStart:clonedStarts) {
                if (!doneScripts.contains(seqStart)) {
                    doneScripts.add(seqStart);
                    
                    block = buildSingleFragmentMethod(i, seqStart, limit, doneScriptsParas, scriptor, generatorData);
                    
                    builder.append("\n");
                    builder.append(block);
                }
                
            }
            
        }
    }

    static Set<String> statsNames = new HashSet<String>();
    
    static void generator(StackTraceElement[] trace, GeneratorData generatorData, GeneratorDriving scriptor, long ... values) {
        
        String templateMethodName = trace[0].getMethodName();
        
       // System.err.println("template method name "+templateMethodName);
        
        if (generatorData.usages.containsKey(templateMethodName)) {
            generatorData.usages.get(templateMethodName).incrementAndGet();
        } else {
            generatorData.usages.put(templateMethodName,new AtomicInteger(1));
        }        
        
        String methodNameKey = " "+templateMethodName+'('; ///must include beginning and end to ensure match
        String[] paraVals = generatorData.templates.params(methodNameKey);
        String[] paraDefs = generatorData.templates.defs(methodNameKey);
        String comment = "        //"+trace[0].getMethodName()+(Arrays.toString(paraVals).replace('[','(').replace(']', ')'))+"\n";
        
        
        //TODO: each of the para files must be found in order in the para defs!!
        //System.err.println("ParaVals:"+Arrays.toString(paraVals));
        //System.err.println("ParaDefs:"+Arrays.toString(paraDefs));
        
        String statsName = templateMethodName+"Stats"; 
        
        
        
        //debug stats gathering
        if (!statsNames.contains(statsName)) {
            statsNames.add(statsName);
            generatorData.statsBuilder.append("Stats "+statsName+" = new Stats(1000000,1200000);\n");
        }
       
        
        //template details to add as comments
        int token = scriptor.getActiveToken();
        int fieldId = scriptor.getActiveFieldId(); 
        String fieldName = scriptor.getActiveFieldName();     
        comment+="        //name='"+fieldName+"' id="+fieldId+" token="+TokenBuilder.tokenToString(token)+"\n";
    
        
        //replace variables with constants
        String template = generatorData.templates.template(methodNameKey);       
        if (OPTIMIZE_PMAP_READ_WRITE) {
            template = optimizeTemplatePMapReadWrite(generatorData, templateMethodName, template);
        }
    
        long[] data = values;
        int i = data.length;
        while (--i>=0) {
            String hexValue; 
            if (data[i]>Integer.MAX_VALUE || 
                (data[i]<Integer.MIN_VALUE && (data[i]>>>32)!=0xFFFFFFFF)) {
                hexValue = Long.toHexString(data[i])+"L";
            } else {
                hexValue = Integer.toHexString((int)data[i]);
            }
            
            
            
            
            if (REMOVE_ARRAY) {
                String intDictionaryRef = "rIntDictionary["+paraVals[i]+"]";
                String intDictionaryReplace = "i"+hexValue;//used as var name;
                
                String longDictionaryRef = "rLongDictionary["+paraVals[i]+"]";
                String longDictionaryReplace = "l"+hexValue;//used as var name;
                
                
                if (template.contains(intDictionaryRef)) {
                    String varInit = "private int "+intDictionaryReplace+";\n";                                        
                    if (generatorData.dictionaryBuilderInt.indexOf(varInit)<0) {
                        generatorData.dictionaryBuilderInt.append(varInit);
                    }
                    template = template.replace(intDictionaryRef, "dispatch."+intDictionaryReplace);                    
                }
                
                if (template.contains(longDictionaryRef)) {
                    String varInit = "private long "+longDictionaryReplace+";\n";                                        
                    if (generatorData.dictionaryBuilderLong.indexOf(varInit)<0) {
                        generatorData.dictionaryBuilderLong.append(varInit);
                    }
                    template = template.replace(longDictionaryRef, "dispatch."+longDictionaryReplace);                    
                }                
                
            }
                    
            
            template = template.replace(paraVals[i],"0x"+hexValue  
                       +   (ADD_COMMENTS ? ("/*"+paraVals[i]+"="+Long.toString(data[i])+"*/") : "")
                       );
        }
        
        
        
        StringBuilder fieldParaValues = new StringBuilder();
        StringBuilder fieldParaDefs = new StringBuilder();
        generateParameters(paraVals, paraDefs, fieldParaValues, fieldParaDefs, data.length, generatorData.dispatchType);
        
        //accumulate new paras for case method.
        i = data.length;
        while (i<paraVals.length) {
            if (!generatorData.caseParaDefs.contains(paraDefs[i])) {
                               
                
                if (!REMOVE_ARRAY | 
                        (!paraVals[i].equals("dispatch")  && 
                         !paraVals[i].equals("rIntDictionary")  && 
                         !paraVals[i].equals("rLongDictionary"))   ) {
                
                    generatorData.caseParaDefs.add(paraDefs[i]);
                    generatorData.caseParaVals.add(paraVals[i]);
                } else {
                    if (!generatorData.caseParaVals.contains("dispatch")) {
                        generatorData.caseParaDefs.add(generatorData.dispatchType+" dispatch");
                        generatorData.caseParaVals.add("dispatch");
                    }
                }
                
            }
            i++;
        }
    
        String methodName = buildMethodName(generatorData);
                       
        
        if (methodNameKey.contains("Length")) {
            generatorData.fieldMethodBuilder.append("private static void ").append(methodName).append("(").append(fieldParaDefs).append(") {\n");;
            //insert field operator content into method
            if (ADD_COMMENTS) {
                generatorData.fieldMethodBuilder.append(comment);
            }
            generatorData.fieldMethodBuilder.append(template);
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
                additionalComplexity+generatorData.runningComplexity<=GeneratorUtils.COMPLEXITY_LIMITY_PER_METHOD && 
                generatorData.fieldMethodBuilder.length()>0) {
                //this field has the same parameters as the  previous and
                //adding this complexity is under the limit and
                //previous method was appended onto builder
                //so combine this.
                
                //strip off the method close so we can tack some more work in it.
                assert(generatorData.fieldMethodBuilder.toString().endsWith(GeneratorData.END_FIELD_METHOD));
                generatorData.fieldMethodBuilder.setLength(generatorData.fieldMethodBuilder.length()-GeneratorData.END_FIELD_METHOD.length());
                                
                //insert field operator content into method
                if (ADD_COMMENTS) {
                    generatorData.fieldMethodBuilder.append(comment);
                }                
                generatorData.fieldMethodBuilder.append(template);
                
                //close field method
                generatorData.fieldMethodBuilder.append(GeneratorData.END_FIELD_METHOD);
                
                generatorData.runningComplexity += additionalComplexity;
                
                //Do not change lastFieldParaValues
                
            } else {
                
                //method signature line
                generatorData.fieldMethodBuilder.append("private static void ").append(methodName).append("(").append(fieldParaDefs).append(") {\n");
          
                //insert field operator content into method
                if (ADD_COMMENTS) {
                    generatorData.fieldMethodBuilder.append(comment);
                }
                generatorData.fieldMethodBuilder.append(template);
                //close field method
                generatorData.fieldMethodBuilder.append(GeneratorData.END_FIELD_METHOD);
                
                //add call to this method from the group method
                generatorData.groupMethodBuilder.append("    ").append(methodName).append("(").append(curFieldParaValues).append(");\n");
    
                generatorData.runningComplexity = additionalComplexity;
                generatorData.lastFieldParaValues = curFieldParaValues;
            }
        }
    }

    private static String optimizeTemplatePMapReadWrite(GeneratorData generatorData, String templateMethodName,
            String template) {
        
        //NOTE: remove this entire block and escape early if the unsupported optional decimals are used!!!
        //Must disable this if we ever see an optional decimal. TODO: X, this could allow a few optional cases with more thought.
        if (templateMethodName.contains("OptionalMantissa") && !templateMethodName.contains("OptionalMantissaDelta")) {
            //TODO: B, need to do this adjust reader.pmapIdxBitBlock -= (1<<16);  but Dont detect this here do it early when we start the script for this fragment.
            generatorData.readerPmapBit = Integer.MIN_VALUE;//used as disable flag
            //Optimization was ok up to this point, after here it will use the slower safe method.                        
        }
        
        
        
        //optimizes the pmap reading logic by removing the extra shift counter and 
        //replacing it with constants
        if (Integer.MIN_VALUE!=generatorData.readerPmapBit) {
            
            //For reader
            if (template.contains("PrimitiveReader.readPMapBit(reader)")) {
                int mapTmp;
                if ((mapTmp = generatorData.readerPmapBit--)<0) {
                    //next up
                    template = template.replace("PrimitiveReader.readPMapBit(reader)",  "PrimitiveReader.readPMapBitNextByte(reader)");               
                    generatorData.readerPmapBit=5;
                } else {
                    //normal bit
                    template = template.replace("PrimitiveReader.readPMapBit(reader)",  "((1<<"+mapTmp+") & reader.pmapIdxBitBlock)");                              
                }  
            }
          //  assert(!template.contains("PrimitiveReader.readPMapBit")) : "check for exact match of arguments.";
            
            //For writer
            //unlike reader there will be two writers 1 and 0 so the counting will be more difficult.
            //to solve this we use two independent counters
            if (template.contains("PrimitiveWriter.writePMapBit((byte)0, writer);")) {
                //all calls to the write pmap 0 bit can be removed because zero has no side effect
                //but we do need to keep track of when the next byte must be flushed
                
                int mapTmp; 
                if ((mapTmp = generatorData.writerPmapBit0--)<=0) {
                    //next up
                    template = template.replace("PrimitiveWriter.writePMapBit((byte)0, writer);", "PrimitiveWriter.writeNextPMapByte((byte)0, writer);");               
                    generatorData.writerPmapBit0=6;
                } else {
                    //zero bit does nothing so we remove the call
                    template = template.replace("PrimitiveWriter.writePMapBit((byte)0, writer);",  "");
                              
                }  
                
            }
            //                     PrimitiveWriter.writePMapBit((byte)1, writer);
            if (template.contains("PrimitiveWriter.writePMapBit((byte)1, writer);")) {
                int mapTmp; 
                if ((mapTmp = generatorData.writerPmapBit1--)<=0) {
                    //next up
                    template = template.replace("PrimitiveWriter.writePMapBit((byte)1, writer);", "PrimitiveWriter.writeNextPMapByte((byte)1, writer);");               
                    generatorData.writerPmapBit1=6;
                } else {
                    
                    template = template.replace("PrimitiveWriter.writePMapBit((byte)1, writer);",  "writer.pMapByteAccum |= (((byte)1) << "+mapTmp+");");
                              
                }  
            }
         //   assert(!template.contains("PrimitiveWriter.writePMapBit")) : "check for exact match of arguments.";
            
            
            
        }
        return template;
    }



}
