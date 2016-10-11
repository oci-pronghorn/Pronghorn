package com.ociweb.pronghorn.stage.generator;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.phast.PhastDecoder;
import java.io.IOException;

import static com.ociweb.pronghorn.util.Appendables.appendClass;
import static com.ociweb.pronghorn.util.Appendables.appendStaticCall;
import static com.ociweb.pronghorn.util.Appendables.appendValue;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import java.nio.channels.Pipe;

public class PhastDecoderStageGenerator extends TemplateProcessGeneratorLowLevelWriter {

    private final Class decoder = PhastDecoder.class;
    private final Class blobReader = DataInputBlobReader.class;
    //DataInputBlobReader reader = new DataInputBlobReader();
    private final String methodScope = "public";

    //field names
    private final String longDictionaryName = "longDictionary";
    private final String intDictionaryName = "intDictionary";
    private final String readerName = "reader";
    private final String mapName = "map";
    private final String indexName = "idx";
    private final String longValueName = "longVal";
    private final String longValueArrayName = "longArrVal";
    private final String intValueName = "intVal";
    private final String defaultIntDictionaryName = "intDefaults";
    private final String defaultLongDictionaryName = "longDefaults";
    private final String bitMaskName = "bitMask";
    private final String inPipeName = "input";
    private final boolean generateRunnable;

    public PhastDecoderStageGenerator(MessageSchema schema, Appendable target) {
        this(schema, target, false);
    }


    public PhastDecoderStageGenerator(MessageSchema schema, Appendable target, boolean generateRunnable, boolean scopeProtected) {
        super(schema, target, generateClassName(schema)+(generateRunnable ? "" : "Stage"),
                generateRunnable ? "implements Runnable" : "extends PronghornStage",
                generateRunnable ? null : "output",
                scopeProtected ? "protected" : "private",
                false, schema.getClass().getPackage().getName()+".build");
        this.generateRunnable = generateRunnable;
    }


    public PhastDecoderStageGenerator(MessageSchema schema, Appendable target, String interitance, boolean scopeProtected) {
        super(schema, target, generateClassName(schema),  interitance,null,
                scopeProtected ? "protected" : "private",
                false, schema.getClass().getPackage().getName()+".build");
        this.generateRunnable = true;
    }

    public PhastDecoderStageGenerator(MessageSchema schema, Appendable target, boolean generateRunnable) {
        this(schema, target, generateRunnable, generateRunnable);
    }

    @Override
    protected void buildConstructors(Appendable target, String className) throws IOException {

        target.append("public ").append(className).append("(");
        if (generateRunnable) {
            target.append(") { \n");

            FieldReferenceOffsetManager from = MessageSchema.from(schema);
            if (!from.hasSimpleMessagesOnly) {
                target.append(tab).append("startup();\n");
            }

        } else {
            target.append(GraphManager.class.getCanonicalName()).append(" gm, ");
            target.append("Pipe<RawDataSchema>  " + inPipeName + ", ");
            Appendables.appendClass(target, Pipe.class, schema.getClass()).append(" ").append(pipeVarName).append(") {\n");

            target.append(tab).append("super(gm," + inPipeName + ",").append(pipeVarName).append(");\n");
            target.append(tab).append("this.").append(pipeVarName).append(" = ").append(pipeVarName).append(";\n");
            target.append(tab);
            Appendables.appendStaticCall(target, Pipe.class, "from").append(pipeVarName).append(").validateGUID(FROM_GUID);\n");
            target.append(tab + "this." + inPipeName + " = " + inPipeName + ";\n");
        }
        target.append(tab + intDictionaryName + " = new int[150];\n");
        target.append(tab + longDictionaryName + " = new long[150];\n");
        target.append(tab + defaultIntDictionaryName + " = FROM.newIntDefaultsDictionary();\n");
        target.append(tab + defaultLongDictionaryName + " = FROM.newLongDefaultsDictionary();\n");
        target.append("}\n\n");

    }

    private static String generateClassName(MessageSchema schema) {
        if (schema instanceof MessageSchemaDynamic) {
            String name = MessageSchema.from(schema).name.replaceAll("/", "").replaceAll(".xml", "")+"Decoder";
            if (Character.isLowerCase(name.charAt(0))) {
                return Character.toUpperCase(name.charAt(0))+name.substring(1);
            }
            return name;
        } else {
            return (schema.getClass().getSimpleName().replace("Schema", ""))+"Writer";
        }
    }


    @Override
    protected void additionalImports(MessageSchema schema, Appendable target) {
        try {
            target.append("import ").append(schema.getClass().getCanonicalName()).append(";\n");
            target.append("import com.ociweb.pronghorn.stage.phast.PhastDecoder;\n");
            target.append("import ").append(DataInputBlobReader.class.getCanonicalName()).append(";\n");
            target.append("import ").append(PronghornStage.class.getCanonicalName()).append(";\n");
            target.append("import java.util.Arrays;\n");
            target.append("import com.ociweb.pronghorn.pipe.*;\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    protected void generateStartup(Appendable target){
        try{
            target.append("\npublic void startup(){\n");
            target.append("}\n");
        }
        catch (IOException e) {
        throw new RuntimeException(e);
        }
    }

    @Override
    protected void bodyOfNextMessageIdx(Appendable target) throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        
        int[] tokens = from.tokens;
        long[] scriptIds = from.fieldIdScript;
        String[] scriptNames = from.fieldNameScript;
        int[] intDict = from.newIntDefaultsDictionary();
        long[] longDict = from.newLongDefaultsDictionary();
        int i = tokens.length;
        int startsCount = MessageSchema.from(schema).messageStarts().length;

        if (startsCount == 1) {
            target.append(tab).append("return ");
            Appendables.appendValue(target, MessageSchema.from(schema).messageStarts()[0]).append(";\n");
        } else {
            target.append(tab).append("return ");

            if (null == pipeVarName) {
                if (!(schema instanceof MessageSchemaDynamic)) {
                    target.append(schema.getClass().getSimpleName()).append(".");
                }

                target.append("FROM");
            } else {
                Appendables.appendStaticCall(target, Pipe.class, "from").append(pipeVarName).append(")");
            }

            target.append(".messageStarts[");
            Appendables.appendValue(target, startsCount).append("];\n");
        }
    }

    @Override
    protected void bodyOfBusinessProcess(Appendable target, int cursor, int firstField, int fieldCount) throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        //cursor for generating the method after loop
        int cursor2 = cursor;

        int[] tokens = from.tokens;
        long[] scriptIds = from.fieldIdScript;
        String[] scriptNames = from.fieldNameScript;

        //these fields on if supporting preamble
        //int map = from.preambleOffset;
        //long bitMask = from.templateOffset;

        //make reader
        target.append("DataInputBlobReader<RawDataSchema> " + readerName + " = Pipe.inputStream(" + inPipeName + ");\n");
        //this will keep track of variable names
        StringBuilder argumentList = new StringBuilder();

        //bitmask goes here
        target.append(tab + "long " + bitMaskName + " = 1;\n");
        //recieve pmap
        decodePmap(target);
        //pass over group tag 0x10000
        cursor++;

        //TODO: remove these
        //target.append("System.out.println(Arrays.toString(intDefaults));");
        //target.append("System.out.println(Arrays.toString(longDefaults));");
        for (int f = cursor; f < (firstField+fieldCount); f++) {
            int token = from.tokens[cursor];
            int pmapType = TokenBuilder.extractType(token);
            //if (TypeMask.isOptional(pmapType) == true){
                //TODO: support optional fields.
            //}
            if (TypeMask.isInt(pmapType) == true) {
                target.append(tab + "int " + scriptNames[f] + " = ");
                int oper = TokenBuilder.extractOper(token);
                switch (oper) {
                    case OperatorMask.Field_Copy:
                        decodeCopyIntGenerator(target, f);
                        break;
                    case OperatorMask.Field_Constant:
                        //this intentionally left blank, does nothing if constant
                        break;
                    case OperatorMask.Field_Default:
                        decodeDefaultIntGenerator(target, f);
                        break;
                    case OperatorMask.Field_Delta:
                        decodeDeltaIntGenerator(target, f);
                        break;
                    case OperatorMask.Field_Increment:
                        decodeIncrementIntGenerator(target, f);
                        break;
                    case OperatorMask.Field_None:
                        target.append("0;//no oper currently not supported.\n");
                        break;
                    default: {
                        target.append("//here as placeholder, this is unsupported\n");
                    }
                }
                target.append(tab + bitMaskName + " = " + bitMaskName + " << 1;\n");
            } //if long, goes to switch to find correct operator to call 
            else if (TypeMask.isLong(pmapType) == true) {
                target.append(tab + "long " + scriptNames[f] + " = ");
                int oper = TokenBuilder.extractOper(token);
                switch (oper) {
                    case OperatorMask.Field_Copy:
                        decodeDeltaLongGenerator(target, f);
                        break;
                    case OperatorMask.Field_Constant:
                        //this intentionally left blank, does nothing if constant
                        break;
                    case OperatorMask.Field_Default:
                        decocdeDefaultLongGenerator(target, f);
                        break;
                    case OperatorMask.Field_Delta:
                        decodeDeltaLongGenerator(target, f);
                        break;
                    case OperatorMask.Field_Increment:
                        decodeIncrementLongGenerator(target, f);
                        break;
                }
                target.append(tab + bitMaskName + " = " + bitMaskName + " << 1;\n");
            } //if string
            else if (TypeMask.isText(pmapType) == true) {
                target.append(tab + "String " + scriptNames[f] + " = ");
                decodeStringGenerator( target);
                target.append(tab + bitMaskName + " = " + bitMaskName + " << 1;\n");
            } else {
                target.append("Unsupported data type " + pmapType + "\n");
            }
            cursor++;
            argumentList.append(scriptNames[f]);
            if (f != (firstField+fieldCount) - 1){
                argumentList.append(',');
            }
        }

        //open method to call with the variable names
        appendWriteMethodName(target.append(tab), cursor2).append("(");
        target.append(argumentList);
        target.append(");\n");

        target.append(tab + "Pipe.publishWrites(" + pipeVarName + ");");
    }

    protected void additionalMethods(Appendable target) throws IOException {
        //blank to remove request shut down method from parent class
    }

    @Override
    protected void additionalMembers(Appendable target) throws IOException {
        /*
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        int[] tokens = from.tokens;
        long[] scriptIds = from.fieldIdScript;
        String[] scriptNames = from.fieldNameScript;
        int[] intDict = from.newIntDefaultsDictionary();
        long[] longDict = from.newLongDefaultsDictionary();
        int i = tokens.length;

        while (--i >= 0) {
            int type = TokenBuilder.extractType(tokens[i]);

            if (TypeMask.isLong(type)) {
                target.append("private long ").append(scriptNames[i]).append(";\n");
            } else if (TypeMask.isInt(type)) {
                target.append("private int ").append(scriptNames[i]).append(";\n");
            } else if (TypeMask.isText(type)) {
                target.append("private String ").append(scriptNames[i]).append(";\n");
            }
        }
        */
        target.append("private long[] " + longDictionaryName + ";\n");
        target.append("private int[] " +intDictionaryName + ";\n");
        target.append("private long[] " +defaultLongDictionaryName + ";\n");
        target.append("private int[] " +defaultIntDictionaryName + ";\n");
        target.append("private Pipe<RawDataSchema> " + inPipeName + ";\n");

    }

    
    protected void decodePmap(Appendable target){
        try {
            target.append(tab + "long " + mapName + " = ");
            appendStaticCall(target, blobReader, "readPackedLong").append(readerName).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    protected void decodeIncrementLongGenerator(Appendable target, int index){
        try {
            appendStaticCall(target, decoder, "decodeIncrementLong").append(longDictionaryName).append(", ").append(mapName).append(", ").append(Integer.toString(index)).append(", ").append(bitMaskName).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    protected void decocdeDefaultLongGenerator(Appendable target, int index){
        try {
            appendStaticCall(target, decoder, "decodeDefaultLong").append(readerName).append(", ").append(mapName).append(", ").append(defaultIntDictionaryName).append(", ").append(bitMaskName).append(", ").append(Integer.toString(index)).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    protected void decodeStringGenerator(Appendable target) {
        try {
            appendStaticCall(target, decoder, "decodeString").append(readerName).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void decodeDeltaLongGenerator(Appendable target, int index) {
        try {
            appendStaticCall(target, decoder, "decodeDeltaLong").append(longDictionaryName).append(", ").append(readerName).append(", ").append(mapName).append(", ").append(Integer.toString(index)).append(", ").append(bitMaskName).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void decodeDefaultIntGenerator(Appendable target, int index) {
        try {
            appendStaticCall(target, decoder, "decodeDefaultInt").append(readerName).append(", ").append(mapName).append(", ").append(defaultIntDictionaryName).append(", ").append(bitMaskName).append(", ").append(Integer.toString(index)).append(", ").append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void decodeDeltaIntGenerator( Appendable target, int index) {
        try {
            appendStaticCall(target, decoder, "decodeDeltaInt").append(intDictionaryName).append(", ").append(readerName).append(", ").append(mapName).append(", ").append(Integer.toBinaryString(index)).append(", ").append(bitMaskName).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
    MAY OR MAY NOT NEED THESE TWO:
    
    protected void decodeIncIntGenerator(MessageSchema schema, Appendable target) {
       try {
           appendStaticCall(target, decoder, "decodeDeltaInt").append(intDictionaryName).append(", ").append(readerName).append(", ").append(mapName).append(", ").append(indexName).append(", ").append(bitMaskName).append(");\n");
       } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    protected void decodeIncIntSlowGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, decoder, "decodeDeltaInt").append(intDictionaryName).append(", ").append(readerName).append(", ").append(mapName).append(", ").append(indexName).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
*/
    protected void decodeCopyIntGenerator(Appendable target, int index) {
        try {
            appendStaticCall(target, decoder, "decodeCopyInt").append(intDictionaryName).append(", ").append(readerName).append(", ").append(mapName).append(", ").append(Integer.toString(index)).append(", ").append(bitMaskName).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void decodeIncrementIntGenerator(Appendable target, int index) {
        try {
            appendStaticCall(target, decoder, "decodeIncrementInt").append(intDictionaryName).append(", ").append(mapName).append(", ").append(Integer.toString(index)).append(", ").append(bitMaskName).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void decodePresentIntGenerator(Appendable target) {
        try {
            appendStaticCall(target, decoder, "decodePresentInt").append(", ").append(readerName).append(", ").append(mapName).append(", ").append(bitMaskName).append(");\n");
        } catch (IOException e) {
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
