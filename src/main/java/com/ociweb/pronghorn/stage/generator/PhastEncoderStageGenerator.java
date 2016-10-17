package com.ociweb.pronghorn.stage.generator;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import java.io.IOException;
import com.ociweb.pronghorn.stage.phast.PhastEncoder;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;

import static com.ociweb.pronghorn.util.Appendables.*;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.token.*;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelReader;

import java.nio.channels.Pipe;
import java.util.logging.Level;

import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class PhastEncoderStageGenerator extends TemplateProcessGeneratorLowLevelReader {

    private final Class encoder = PhastEncoder.class;
    private final Class blobWriter = DataOutputBlobWriter.class;
    private final Appendable bodyTarget;
    private final String defLongDictionaryName = "defLongDictionary";
    private final String defIntDictionaryName = "defIntDictionary";
    //short not supported yet
    //private final String defShortDictionaryName = "defShortDictiornary";
    private final String longDictionaryName = "previousLongDictionary";
    private final String intDictionaryName = "previousIntDictionary";
    private final String shortDictionaryName = "previousShortDictionary";
    private final String writerName = "writer";
    private final String pmapName = "map";
    private final String indexName = "idx";
    private final String bitMaskName = "bitMask";
    private final String intValueName = "intVal";
    private final String inPipeName = "input";
    private final String outPipeName = "output";
    private final boolean generateRunnable;

    private static final String tab = "    ";
    private static final Logger logger = LoggerFactory.getLogger(PhastEncoderStageGenerator.class);
    private static final String DECODER_CLASS_NAME = "com.ociweb.pronghorn.stage.phast.PhastEncoder";
    private static final String DATA_BLOB_WRITER_CLASS = "com.ociweb.pronghorn.pipe.DataOutputBlobWriter";

//    public PhastEncoderStageGenerator(MessageSchema schema, Appendable bodyTarget) {
//        super(schema, bodyTarget);
//        this.bodyTarget = bodyTarget;
//        generateRunnable = false;
//    }

    public PhastEncoderStageGenerator(MessageSchema schema, Appendable bodyTarget) {
        super(schema, bodyTarget, generateClassName(schema) + " extends PronghornStage",
             generateClassName(schema), schema.getClass().getPackage().getName()+".build");
        this.bodyTarget = bodyTarget;
        generateRunnable = false;
    }

    private static String generateClassName(MessageSchema schema) {
        if (schema instanceof MessageSchemaDynamic) {
            String name = MessageSchema.from(schema).name.replaceAll("/", "").replaceAll(".xml", "")+"EncoderStage";
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
            target.append("import " + DECODER_CLASS_NAME + ";\n");
            target.append("import " + DATA_BLOB_WRITER_CLASS + ";\n");
            target.append("import java.util.Arrays;\n");
            target.append("import com.ociweb.pronghorn.pipe.*;\n");
            target.append("import com.ociweb.pronghorn.stage.PronghornStage;\n\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    protected void additionalMembers(Appendable target) throws IOException {
        target.append("long[] " + longDictionaryName + ";\n");
        target.append("int[] " + intDictionaryName + ";\n");
        target.append("long[] " + defLongDictionaryName + ";\n");
        target.append("int[] " + defIntDictionaryName + ";\n");
        //isntantiate pipe
        target.append("DataOutputBlobWriter<" + schema.getClass().getSimpleName() + "> " + writerName + ";\n");
        //target.append("private Pipe<MessageSchemaDynamic> " + inPipeName + ";\n");
        target.append("private Pipe<RawDataSchema> " + outPipeName + ";\n");
    }

    private void generateVariables(MessageSchema schema, Appendable target) throws IOException {
        target.append(tab + "long " + pmapName + " = 0;\n");
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

            target.append("Pipe<MessageSchemaDynamic>  " + inPipeName + ", ");
            target.append("Pipe<RawDataSchema> " + outPipeName + ") {\n");
            //Appendables.appendClass(target, Pipe.class, schema.getClass()).append(" ").append(outPipeName).append(") {\n");

            target.append(tab).append("super(gm," + inPipeName + ",").append(outPipeName).append(");\n");
            target.append(tab).append("this." + outPipeName + " = " + outPipeName + ";\n");
            target.append(tab + "this." + inPipeName + " = " + inPipeName + ";\n");
            target.append(tab);
            Appendables.appendStaticCall(target, Pipe.class, "from").append(outPipeName).append(").validateGUID(FROM_GUID);\n");

        }
        target.append(tab + intDictionaryName + " = FROM.newIntDefaultsDictionary();\n");
        target.append(tab + longDictionaryName + " = FROM.newLongDefaultsDictionary();\n");
        target.append(tab + defIntDictionaryName + " = FROM.newIntDefaultsDictionary();\n");
        target.append(tab + defLongDictionaryName + " = FROM.newLongDefaultsDictionary();\n");
        target.append("}\n\n");
    }

    // BodyBuilder OverRide. Lots of Good stuff goes here
    // Creates Pmap for encoding
    @Override
    protected void bodyBuilder(MessageSchema schema, int cursor, int fragmentParaCount, CharSequence[] fragmentParaTypes, CharSequence[] fragmentParaArgs, CharSequence[] fragmentParaSuff) {
        //create FROM which is generated from the schema provided.
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        //incremenent to pass over the group start at the begging of array
        cursor++;
        int curCursor = cursor;
        int curCursor2 = cursor;

        //get bitmask
        int bitMask = from.templateOffset;

        //this try catches all IO problems and throws an error if the file does not exist
        try {

            //call to instantiate dictionaries
            generateVariables(schema, bodyTarget);
            bodyTarget.append(tab + "DataOutputBlobWriter<RawDataSchema> " + writerName + " = Pipe.outputStream("
                    + outPipeName + ");\n");

            //traverse all tokens and print out a pmap builder for each of them
            int i = fragmentParaCount - 1;
            while (i >= 0) {
                int token = from.tokens[curCursor];
                int pmapType = TokenBuilder.extractType(token);

                String varName = new StringBuilder().append(fragmentParaArgs[i]).append(fragmentParaSuff[i]).toString();
                String varType = new StringBuilder().append(fragmentParaTypes[i]).toString();

                boolean isNull = false;

                if (TypeMask.isOptional(pmapType)) {
                    isNull = true;
                }

                //call appropriate pmap builder according to type
                if (varType.equals("int")) {
                    bodyTarget.append(tab + pmapName + " = ");
                    encodePmapBuilderInt(schema, bodyTarget, token, i, varName, isNull);

                } else if (varType.equals("long")) {
                    bodyTarget.append(tab + pmapName + " = ");
                    encodePmapBuilderLong(schema, bodyTarget, token, i, varName, isNull);

                } else if (varType.equals("StringBuilder")) {
                    bodyTarget.append(tab + pmapName + " = ");
                    encodePmapBuilderString(schema, bodyTarget, token, varName);

                } else {
                    bodyTarget.append("caught by nothing\n");
                }

                curCursor += TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];
                i--;
            }

            //taking in pmap
            bodyTarget.append(tab + "DataOutputBlobWriter.writePackedLong(" + writerName + ", " + pmapName + ");\n");
            //instantiating bimask
            bodyTarget.append(tab + "long " + bitMaskName + " = 1;\n");
            //line break for readability
            bodyTarget.append("\n");

            //number of shifts if it is optional or not
            int numShifts = 0;
            //traverses all data and pulls them off the pipe
            for (int paramIdx = 0; paramIdx < fragmentParaCount; paramIdx++) {
                int token = from.tokens[curCursor2];
                int pmapType = TokenBuilder.extractType(token);
                String varName = new StringBuilder().append(fragmentParaArgs[paramIdx]).append(fragmentParaSuff[paramIdx]).toString();
                String varType = new StringBuilder().append(fragmentParaTypes[paramIdx]).toString();
                //if int, goes to switch to find correct operator to call
                if (TypeMask.isInt(pmapType) == true) {
                    int oper = TokenBuilder.extractOper(token);
                    numShifts = 1;
                    switch (oper) {
                        case OperatorMask.Field_Copy:
                            copyIntGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName);
                            break;
                        case OperatorMask.Field_Constant:
                            //this intentionally left blank, does nothing if constant
                            break;
                        case OperatorMask.Field_Default:
                            encodeDefaultIntGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName);
                            break;
                        case OperatorMask.Field_Delta:
                            encodeDeltaIntGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName);
                            break;
                        case OperatorMask.Field_Increment:
                            incrementIntGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName);
                            break;
                        case OperatorMask.Field_None:
                            bodyTarget.append("//no oper, not supported yet.\n");
                            break;
                        default: {
                            bodyTarget.append("Unsupported Operator Type " + pmapType + "\n");
                        }
                    }
                } //if long, goes to switch to find correct operator to call 
                else if (TypeMask.isLong(pmapType) == true) {
                    int oper = TokenBuilder.extractOper(token);
                    numShifts = 1;
                    switch (oper) {
                        case OperatorMask.Field_Copy:
                            copyLongGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName);
                            break;
                        case OperatorMask.Field_Constant:
                            //this intentionally left blank, does nothing if constant
                            break;
                        case OperatorMask.Field_Default:
                            encodeDefaultLongGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName);
                            break;
                        case OperatorMask.Field_Delta:
                            encodeDeltaLongGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName);
                            break;
                        case OperatorMask.Field_Increment:
                            incrementLongGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName);
                            break;
                    }
                } //if string
                else if (TypeMask.isText(pmapType) == true) {
                    numShifts = 1;
                    encodeStringGenerator(schema, bodyTarget, varName);
                } else {
                    bodyTarget.append("Unsupported data type " + pmapType + "\n");
                }
                if (paramIdx != (fragmentParaCount - 1)) {
                    bodyTarget.append(tab + bitMaskName + " = " + bitMaskName + " << " + numShifts + ";\n");
                }
                curCursor2 += TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];
                //pipeVarName needs protected status
                //bodyTarget.append(tab + "Pipe.confirmLowLevelRead(" + inPipeName + ", )" +  + ");\n");
                //bodyTarget.append(tab + "Pipe.releaseLock(" + )
            }
        } catch (IOException e) {
            java.util.logging.Logger.getLogger(PhastEncoderStageGenerator.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    //  BuilderInt Factory
    protected void encodePmapBuilderInt(MessageSchema schema, Appendable target, int token, int index, String valName, boolean isNull) {
        try {
            //TODO: add support for isnull
            appendStaticCall(target, encoder, "pmapBuilderInt")
                    .append(pmapName).append(", ")
                    .append("0x" + Integer.toHexString(token)).append(", ")
                    .append(valName).append(", ")
                    .append(intDictionaryName + "[" + index + "]").append(", ")
                    .append(defIntDictionaryName + "[" + index + "]").append(", ")
                    .append(Boolean.toString(isNull))
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // builderLong Factory
    protected void encodePmapBuilderLong(MessageSchema schema, Appendable target, int token, int index, String valName, boolean isNull) {
        try {
            appendStaticCall(target, encoder, "pmapBuilderLong")
                    .append(pmapName).append(", ")
                    .append("0x" + Integer.toHexString(token)).append(", ")
                    .append(valName).append(", ")
                    .append(longDictionaryName + "[" + index + "]").append(", ")
                    .append(defLongDictionaryName + "[" + index + "]").append(", ")
                    .append(Boolean.toString(isNull))
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // BuilderString Factory
    protected void encodePmapBuilderString(MessageSchema schema, Appendable target, int token, String valName) {
        try {
            appendStaticCall(target, encoder, "pmapBuilderString")
                    .append(pmapName).append(", ")
                    .append("0x" + Integer.toHexString(token)).append(", ")
                    .append("(" + valName + " == null)")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IntPresent Factory
    protected void encodeIntPresentGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeIntPresent")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DeltaInt Factory
    protected void encodeDeltaIntGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDeltaInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DeltaLong Factory
    protected void encodeDeltaLongGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDeltaLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // String Factory
    protected void encodeStringGenerator(MessageSchema schema, Appendable target, String valName) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeString")
                    .append(writerName).append(", ")
                    .append(valName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IncrementInt Factory
    protected void incrementIntGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "incrementInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx))
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CopyInt Factory
    protected void copyIntGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "copyInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DefaultInt Factory
    protected void encodeDefaultIntGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDefaultInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // LongPresent Factory
    protected void encodeLongPresentGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeLongPresentGenerator")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(value)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IncrementLong Factory
    protected void incrementLongGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "incrementLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx))
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CopyLong Factory
    protected void copyLongGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "copyLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DefaultLong Factory
    protected void encodeDefaultLongGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDefaultLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ShortPresent Factory
    protected void encodeShortPresentGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeShortPresent")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName)
                    .append(value).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IncrementShort Factory
    protected void incrementShortGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "incrementShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CopyShort Factory
    protected void copyShortGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "copyShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DefaultShort Factory
    protected void encodeDefaultShortGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDefaultShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DeltaShort Factory
    protected void encodeDeltaShortGenerator(MessageSchema schema, Appendable target, int idx, String value) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDeltaShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(value).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
