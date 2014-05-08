package com.ociweb.jfast.generator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.loader.BalancedSwitchGenerator;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.SourceTemplates;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTReaderDispatchBase;
import com.ociweb.jfast.stream.FASTReaderDispatchTemplates;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;

public class FASTReaderDispatchGenerator extends FASTReaderInterpreterDispatch {



    //TODO: B, must reduce size of large methods as well.
    // TODO: B, this code generation must take place when loading the catalog
    // binary file. So catalog file can be the same cross languages.
    // TODO: B, copy other dispatch and use it for code generation, if possible
    // build generator that makes use of its own source as template.
    // TODO: C, code does not support final in signatures, this would be nice to have
    //TODO: A, must gather code and based on complexity group into functions to reduce total calls.
    
    
    
    
    
    private static final String GROUP_METHOD_NAME = "grp";

    SourceTemplates templates;
    
    StringBuilder fieldBuilder;
    StringBuilder caseBuilder;
    List<String> caseParaDefs = new ArrayList<String>(); 
    List<String> caseParaVals = new ArrayList<String>(); 
    int scriptPos;
    int templateId;
    
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
    
    private String getSingleGroupMethod(List<String> doneScriptsParas) {
        
        String paraDefs = caseParaDefs.toString().substring(1);
        paraDefs = paraDefs.substring(0, paraDefs.length()-1);
        
        String paraVals = caseParaVals.toString().substring(1);
        paraVals = paraVals.substring(0, paraVals.length()-1);
        doneScriptsParas.add(paraVals);
        
        StringBuilder signatureLine = new StringBuilder();
        signatureLine.append("private static int ")
                     .append(GROUP_METHOD_NAME)
                     .append(scriptPos)
                     .append("(")
                     .append(paraDefs)
                     .append(") {\n");
        
        caseBuilder.append("    return ").append(activeScriptCursor).append(";\n");
        
        return signatureLine.toString()+caseBuilder.toString()+caseTail+fieldBuilder.toString();
    }
    
    private void beginSingleGroupMethod(int scriptPos, int templateId) {
        fieldBuilder.setLength(0);
        caseBuilder.setLength(0);
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
    
    private int complexity(CharSequence seq) {
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
    
    
    private void generator(StackTraceElement[] trace, long ... values) {
        
        String methodNameKey = " "+trace[0].getMethodName()+'('; ///must include beginning and end to ensure match
        String[] paraVals = templates.params(methodNameKey);
        String[] paraDefs = templates.defs(methodNameKey);
        String comment = "        //"+trace[0].getMethodName()+(Arrays.toString(paraVals).replace('[','(').replace(']', ')'))+"\n";
        
  //      assert(params.length<values.length): "Bad params for "+methodNameKey;
        
        //replace variables with constants
        String template = templates.template(methodNameKey);
        
        //TODO: A, sum total complexity and write full method when it reaches 18
        //int complexity = complexity(template);
        //System.err.println("///////////////complexity "+complexity);
        
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
            
            template = template.replace(paraVals[i],strData+"/*"+paraVals[i]+"*/");
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
        
        fieldCount++;
        String field = Integer.toHexString(fieldCount);
        while (field.length()<3) {
            field = "0"+field;
        }
        field = fieldPrefix+"_"+field;
        
        if (methodNameKey.contains("Length")) {
            fieldBuilder.append("private static boolean ");
            caseBuilder.append("    if (").append(field).append("(").append(fieldParaValues).append(")) {return "+(activeScriptCursor+1)+";};\n");
        } else {
            fieldBuilder.append("private static void ");
            caseBuilder.append("    ").append(field).append("(").append(fieldParaValues).append(");\n");
        }
        fieldBuilder.append(field).append("(").append(fieldParaDefs).append(") {\n").append(comment).append(template).append("};\n");
        
        
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
    
    public void generateAllGroupMethods(TemplateCatalog catalog, List<Integer> doneScripts, List<String> doneScriptsParas, Appendable builder) throws IOException {
        
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
        dispatchReadByToken();
        return getSingleGroupMethod(doneScriptsParas);
    }
    
    private void generateEntryDispatchMethod(List<Integer> doneScripts, List<String> doneScriptsParas, Appendable builder) throws IOException {
        assert(doneScripts.size() == doneScriptsParas.size());
        int j = 0;
        int[] doneValues = new int[doneScripts.size()];
        String[] doneCode = new String[doneScripts.size()];
        for(Integer d:doneScripts) {
            doneCode[j] = "assert (gatherReadData(reader, activeScriptCursor));\n\ractiveScriptCursor="+GROUP_METHOD_NAME+d+"("+doneScriptsParas.get(j)+");\n";
            doneValues[j++] = d;
        }
        BalancedSwitchGenerator bsg = new BalancedSwitchGenerator();
        builder.append("public final boolean dispatchReadByToken() {\n");
        builder.append("    doSequence = false;\n");
        builder.append("    int x = activeScriptCursor;\n");
        bsg.generate("    ",builder, doneValues, doneCode);
        builder.append("    return doSequence;\n");
        builder.append("}\n");

    }

    public void generateFullReaderSource(TemplateCatalog catalog, Appendable target) throws IOException {
        List<Integer> doneScripts = new ArrayList<Integer>();
        List<String> doneScriptsParas = new ArrayList<String>();
        
        target.append("package com.ociweb.jfast.stream;\n");
        target.append("\n");
        target.append(templates.imports());
        target.append("\n");
        target.append("public final class FASTReaderDispatchGenExample extends FASTReaderDispatchBase {");
        target.append("\n");
        target.append(templates.constructor().replace(FASTReaderDispatchTemplates.class.getSimpleName(),"FASTReaderDispatchGenExample"));
        target.append("\n");
        generateAllGroupMethods(catalog,doneScripts,doneScriptsParas,target);
        
        generateEntryDispatchMethod(doneScripts,doneScriptsParas,target);
        target.append('}');

    }
    
    public void createWriteSourceClassFiles(TemplateCatalog catalog) {
                
        try {
            
            File tempDir = new File(System.getProperty("java.io.tmpdir"));//TODO: need unique directory for this install/catalog.
            String rootName = "FASTReaderDispatchGenExample"; //TODO: pull out as constant.
            //TODO: must create full path to this file.
            
            File path = new File(tempDir,"com");
            path = new File(path,"ociweb");
            path = new File(path,"jfast");
            path = new File(path,"stream");
            path.mkdirs();
            
            
            File sourceTarget = new File(path,rootName+".java");
            
            
            //write source file
            FileWriter writer = new FileWriter(sourceTarget);
            generateFullReaderSource(catalog,writer);
            writer.close();
            
            //write class file
            List<File> toCompile = new ArrayList<File>();
            toCompile.add(sourceTarget);
            compile(toCompile);
            
            System.err.println(sourceTarget);
            
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
        
    }
    
    //TODO: A, Write the soruce to a known folder?
    //TODO: A, Compile from source to another known folder?
    //TODO: A, Use known folder to load class and use?
    
    
    public void compile(Iterable<? extends File> javaFiles) {
        
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (null!=compiler) {

            DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
            StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);
            
            Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromFiles(javaFiles);

            List<String> optionList = new ArrayList<String>();
            optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path")));
            JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, optionList, null, compilationUnits);
            if (task.call()) {
                
                System.out.println("success");
                //success
                
                //use class loader to pull this up.
                
            } else {
                //did not compile due to error
                System.err.println("error in compile  ");
                
                for (Diagnostic<? extends JavaFileObject> x:      diagnostics.getDiagnostics()) {
                    System.err.println(x);
                }
                
            }
            
            
        } else {
            //unable to compile, no compiler
            System.err.println("no compiler");
        }
        
        
    }
    
    @Override
    protected void genReadSequenceClose(int backvalue, FASTReaderDispatchBase dispatch) {
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
    protected boolean genReadLengthDefault(int constDefault,  int jumpToTarget, int[] rbB, PrimitiveReader reader, int rbMask, FASTRingBuffer rbRingBuffer, FASTReaderDispatchBase dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),constDefault,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthIncrement(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTReaderDispatchBase dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target,source,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthCopy(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTReaderDispatchBase dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target,source,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthConstant(int constDefault, int jumpToTarget, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, FASTReaderDispatchBase dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),constDefault,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLengthDelta(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTReaderDispatchBase dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target,source,jumpToTarget);
        return true;
    }

    @Override
    protected boolean genReadLength(int target,  int jumpToTarget, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, int[] rIntDictionary, PrimitiveReader reader, FASTReaderDispatchBase dispatch) {
        sequenceStarts.add(activeScriptCursor+1);
        generator(new Exception().getStackTrace(),target, jumpToTarget);
        return true;
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
    protected void genReadBytesDeltaOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesTailOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesDelta(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesTail(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesNoneOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadBytesNone(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer) {
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
    protected void genReadDictionaryLongReset(int idx, long[] rLongDictionary, long[] rLongInit) {
        generator(new Exception().getStackTrace(),idx);
    }
    
    @Override
    protected void genReadDictionaryIntegerReset(int idx, int[] rIntDictionary, int[] rIntInit) {
        generator(new Exception().getStackTrace(),idx);
    }


    
}
