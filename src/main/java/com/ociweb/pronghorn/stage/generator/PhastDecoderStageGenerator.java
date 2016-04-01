package com.ociweb.pronghorn.stage.generator;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.stage.phast.PhastDecoder;
import java.io.IOException;

import static com.ociweb.pronghorn.util.Appendables.appendClass;
import static com.ociweb.pronghorn.util.Appendables.appendStaticCall;
import static com.ociweb.pronghorn.util.Appendables.appendValue;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.util.Appendables;
import java.nio.channels.Pipe;


public class PhastDecoderStageGenerator extends TemplateProcessGeneratorLowLevelWriter
{
    private final Class decoder = PhastDecoder.class;    
    //DataInputBlobReader reader = new DataInputBlobReader();
    private final Appendable bodyTarget;
    private final String methodScope = "public";
    
    //field names
    private final String longDictionaryName = "longDictionary";
    private final String intDictionaryName = "intDictiornary";
    private final String readerName = "reader";
    private final String mapName = "map";
    private final String indexName = "idx";
    private final String longValueName = "longVal";  
    private final String longValueArrayName = "longArrVal"; 
    private final String intValueName = "intVal";
    private final String intValueArrayName = "intArrVal";
    private final String bitMaskName = "bitMask";
    

    public PhastDecoderStageGenerator(MessageSchema schema, Appendable target, String packageName) {
        super(schema, target, /*isAbstract*/ false, packageName);
        
        this.bodyTarget = target;
    }
    

    @Override
    protected void additionalImports(MessageSchema schema, Appendable target) {
        try {            
            target.append("import ").append(schema.getClass().getCanonicalName()).append(";\n");
            //target.append("import com.ociweb.pronghorn.stage.phast.PhastDecoder;\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    
    @Override
    protected void bodyOfNextMessageIdx(Appendable target) throws IOException 
    {
//        FieldReferenceOffsetManager from = MessageSchema.from(schema);
//        
//        int[] tokens = from.tokens;
//        long[] scriptIds = from.fieldIdScript;
//        String[] scriptNames = from.fieldNameScript;
//        int[] intDict = from.newIntDefaultsDictionary();
//        long[] longDict = from.newLongDefaultsDictionary();
//        int i = tokens.length;
//        int startsCount = MessageSchema.from(schema).messageStarts().length;
//
//        if (startsCount == 1) {
//            target.append(tab).append("return ");
//            Appendables.appendValue(target, MessageSchema.from(schema).messageStarts()[0]).append(";\n");
//        } else {
//            target.append(tab).append("return ");
//
//            if (null == pipeVarName) {
//                if (!(schema instanceof MessageSchemaDynamic)) {
//                    target.append(schema.getClass().getSimpleName()).append(".");
//                }
//
//                target.append("FROM");
//            } else {
//                Appendables.appendStaticCall(target, Pipe.class, "from").append(pipeVarName).append(")");
//            }
//
//            target.append(".messageStarts[(");
//            Appendables.appendValue(target, startsCount).append("];\n");
//        }
    }
    
    @Override
    protected void bodyOfBusinessProcess(Appendable target, int cursor, int firstField, int fieldCount) throws IOException 
    {    
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        PhastDecoder decoder = new PhastDecoder();
        
        int[] tokens = from.tokens;
        long[] scriptIds = from.fieldIdScript;
        String[] scriptNames = from.fieldNameScript;
        int[] intDict = from.newIntDefaultsDictionary();
        long[] longDict = from.newLongDefaultsDictionary();
        int map = from.preambleOffset;
        int bitMask = from.templateOffset;
        
        // Appendables.appendValue(target, "////", cursor,"\n");
        for (int f = firstField; f < fieldCount; f++) 
        {
            int type = TokenBuilder.extractType(tokens[f]);
            if (TypeMask.isLong(type)) 
            {
                //long decodeDeltaLong = PhastDecoder.decodeDeltaLong(longDict, reader, map, f, tokens[f], bitMask);
            }
        }
    }
    
    @Override
    protected void additionalMembers(Appendable target) throws IOException 
    { 
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        int[] tokens = from.tokens;
        long[] scriptIds = from.fieldIdScript;
        String[] scriptNames = from.fieldNameScript;
        int[] intDict = from.newIntDefaultsDictionary();
        long[] longDict = from.newLongDefaultsDictionary();
        int i = tokens.length;

        while (--i >= 0) 
        {
            int type = TokenBuilder.extractType(tokens[i]);

            if (TypeMask.isLong(type))  
            {                
                target.append("private long ").append(scriptNames[i]).append(";\n");                
            }
            else if(TypeMask.isInt(type))
            {
                target.append("private int ").append(scriptNames[i]).append(";\n");
            }
            else if(TypeMask.isText(type))
            {
                target.append("private String ").append(scriptNames[i]).append(";\n");
            }
        }        
    }
    
    protected void decodeDeltaLongGenerator(MessageSchema schema, Appendable target)
    {        
        try 
        {
            appendStaticCall(target, decoder , "decodeDeltaLong").append(longDictionaryName).append(", ").append(readerName).append
                    (", ").append(mapName).append(", ").append(indexName).append(", ").append(longValueName).append(", ").append
                    (bitMaskName).append(");\n");            
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    protected void decodeDefaultIntGenerator(MessageSchema schema, Appendable target)
    {
        try
        {
            appendStaticCall(target, decoder , "decodeDefaultInt").append(readerName).append(", ").append(mapName).append
                    (", ").append(intValueArrayName).append(", ").append(bitMaskName).append(", ").append
                    (indexName).append(", ").append(");\n");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    protected void decodeDeltaIntGenerator(MessageSchema schema, Appendable target)
    {
        try
        {
            appendStaticCall(target, decoder , "decodeDeltaInt").append(intDictionaryName).append(", ").append(readerName).append
                    (", ").append(mapName).append(", ").append(indexName).append(", ").append
                    (bitMaskName).append(", ").append(intValueName).append(");\n");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    protected void decodeIncIntGenerator(MessageSchema schema, Appendable target)
    {
        try
        {
            appendStaticCall(target, decoder , "decodeDeltaInt").append(intDictionaryName).append(", ").append(readerName).append
                    (", ").append(mapName).append(", ").append(indexName).append(", ").append
                    (bitMaskName).append(");\n");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    protected void decodeIncIntSlowGenerator(MessageSchema schema, Appendable target)
    {
        try
        {
            appendStaticCall(target, decoder , "decodeDeltaInt").append(intDictionaryName).append(", ").append(readerName).append
                    (", ").append(mapName).append(", ").append(indexName).append(");\n");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    protected void decodeCopyIntGenerator(MessageSchema schema, Appendable target)
    {
        try
        {
            appendStaticCall(target, decoder , "decodeDeltaInt").append(intDictionaryName).append(", ").append(readerName).append
                    (", ").append(mapName).append(", ").append(indexName).append(", ").append(bitMaskName).append(");\n");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    protected void decodeIncrementIntGenerator(MessageSchema schema, Appendable target)
    {
        try
        {
           appendStaticCall(target, decoder , "decodeDeltaInt").append(intDictionaryName).append(", ").append(mapName).append
                    (", ").append(indexName).append(", ").append(bitMaskName).append(");\n"); 
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    protected void decodePresentIntGenerator(MessageSchema schema, Appendable target)
    {
        try
        {
            appendStaticCall(target, decoder , "decodeDeltaInt").append(", ").append(readerName).append(", ").append(mapName).append
                            (", ").append(bitMaskName).append(");\n");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
//    protected void decodeStringGenerator(MessageSchema schema, Appendable target)
//    {
//        try
//        {
//            
//        }
//        catch (IOException e)
//        {
//            throw new RuntimeException(e);
//        }
//    }
    
    //    @Override
//    protected void processCallerPrep() throws IOException
//    {
//        final FieldReferenceOffsetManager from = MessageSchema.from(schema); 
//        
////        if (buildFullStageWritingToPipe()) {
////            from.appendGUID( bodyTarget.append("private final int[] FROM_GUID = ")).append(";\n");
////        } 
//        
//        
////        bodyTarget.append("private final long BUILD_TIME = ");
////        Appendables.appendValue(bodyTarget, System.currentTimeMillis()).append("L;\n");
////        bodyTarget.append("private static final int ").append(doNothingConstant).append(" = ").append(doNothingConstantValue).append(";\n");
//
//
//        from.appendLongDefaults(bodyTarget.append("private final long[] ").append(longDictionaryName).append(" = ").append(";\n"));
//        from.appendIntDefaults(bodyTarget.append("private final int[] ").append(intDictionaryName).append(" = ").append(";\n"));        
//                
//        
//        
//        bodyTarget.append("\n");
//        
//        bodyTarget.append(methodScope).append(" int nextMessageIdx() {\n");        
//        bodyOfNextMessageIdx(bodyTarget);        
//        bodyTarget.append("}\n");
//        
//        bodyTarget.append("\n");
//        bodyTarget.append("@Override\n");
//        bodyTarget.append("public void run() {\n");
//    }
    
//   @Override
//    protected void processCaller(int cursor) throws IOException
//    {        
//        FieldReferenceOffsetManager from = MessageSchema.from(schema);
//        
//        //appendCaseMsgIdConstant(bodyTarget.append(tab).append(tab).append("case "), cursor, schema).append(":\n");
//
//        if ( FieldReferenceOffsetManager.isTemplateStart(from, cursor) ) {
//            beginMessage(bodyTarget, cursor);
//        }
//        
//        bodyTarget.append(tab).append(tab).append(tab);
//        appendBusinessMethodName(cursor).append("();\n");
//                                       
//                                       
//        //Pipe.confirmLowLevelWrite(input, 8);
//        int fragmentSizeLiteral = from.fragDataSize[cursor];
//        
//        if (buildFullStageWritingToPipe()) {
//            appendStaticCall(bodyTarget.append(tab).append(tab).append(tab), pipeClass, "confirmLowLevelWrite").append(pipeVarName).append(", ");
//            Appendables.appendValue(bodyTarget, fragmentSizeLiteral);
//            bodyTarget.append("/* fragment ");
//            Appendables.appendValue(bodyTarget, cursor).append("  size ");
//            Appendables.appendValue(bodyTarget, from.fragScriptSize[cursor]);
//            bodyTarget.append("*/);\n");
//        }
//        
//                                      
//        bodyTarget.append(tab).append(tab).append("break;\n");
//    }
    
    
    //TODO:...

}
