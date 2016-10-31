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
        super(schema, target, generateClassName(schema) + (generateRunnable ? "" : "Stage"),
                generateRunnable ? "implements Runnable" : "extends PronghornStage",
                generateRunnable ? null : "output",
                scopeProtected ? "protected" : "private",
                false, schema.getClass().getPackage().getName() + ".build");
        this.generateRunnable = generateRunnable;
    }


    public PhastDecoderStageGenerator(MessageSchema schema, Appendable target, String interitance, boolean scopeProtected) {
        super(schema, target, generateClassName(schema), interitance, null,
                scopeProtected ? "protected" : "private",
                false, schema.getClass().getPackage().getName() + ".build");
        this.generateRunnable = true;
    }

    public PhastDecoderStageGenerator(MessageSchema schema, Appendable target, boolean generateRunnable) {
        this(schema, target, generateRunnable, generateRunnable);
    }

    /**
     * This method is to be ovveridden and called on by the super class. It should not be called anywhere but by the
     * super class
     *
     * @param target    where the code is being written in super class
     * @param className the base class name, without extensions or implementations
     * @throws IOException when the target can not be written to
     */
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
        target.append(tab + intDictionaryName + " = FROM.newIntDefaultsDictionary();\n");
        target.append(tab + longDictionaryName + " = FROM.newLongDefaultsDictionary();\n");
        target.append(tab + defaultIntDictionaryName + " = FROM.newIntDefaultsDictionary();\n");
        target.append(tab + defaultLongDictionaryName + " = FROM.newLongDefaultsDictionary();\n");
        target.append("}\n\n");

    }

    /**
     * This method generates a class name based on the input schema
     *
     * @param schema This is an object created from an XML schema file
     */
    private static String generateClassName(MessageSchema schema) {
        if (schema instanceof MessageSchemaDynamic) {
            String name = MessageSchema.from(schema).name.replaceAll("/", "").replaceAll(".xml", "") + "Decoder";
            if (Character.isLowerCase(name.charAt(0))) {
                return Character.toUpperCase(name.charAt(0)) + name.substring(1);
            }
            return name;
        } else {
            return (schema.getClass().getSimpleName().replace("Schema", "")) + "Writer";
        }
    }

    /**
     * This method overrides from the super class, where it is called to add imports. This method should not be called
     * from anywhere but the super class.
     *
     * @param schema the schema from super class
     * @param target the Appendable target from super class
     */
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

    /**
     * This method is where the startup is overriden in the printed out code. Called from teh super class, and should
     * not be called from elsewhere.
     *
     * @param target
     */
    @Override
    protected void generateStartup(Appendable target) {
        try {
            target.append("\npublic void startup(){\n");
            target.append("}\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is to be ovveridden and called on by the super class. It should not be called anywhere but by the
     * super class. This addes to the loop logic in the run method in the printed out code.
     *
     * @param target The target that the code is being written to
     */
    @Override
    protected void additionalLoopLogic(Appendable target) {
        try {
            target.append(" && Pipe.contentRemaining(" + inPipeName + ") > 0");
        } catch (IOException e) {
            e.printStackTrace();
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

    /**
     * This method is called from the super class to make the body of the printed out code. It should only be called
     * from the super class.
     *
     * @param target
     * @param cursor
     * @param firstField
     * @param fieldCount
     * @throws IOException
     */
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
        //this will keep track of variable names so they can be called as arguments in a later method
        StringBuilder argumentList = new StringBuilder();

        //bitmask goes here
        target.append(tab + "long " + bitMaskName + " = 1;\n");
        //recieve pmap
        decodePmap(target);
        //pass over group tag 0x10000
        cursor++;
        for (int f = cursor; f < (firstField + fieldCount); f++) {
            int numShifts = 1;
            int token = from.tokens[cursor];
            int pmapType = TokenBuilder.extractType(token);
            if(TypeMask.isOptional(pmapType))
                numShifts++;

            if (TypeMask.isInt(pmapType) == true) {
                target.append(tab + "int " + scriptNames[f] + " = ");
                int oper = TokenBuilder.extractOper(token);
                switch (oper) {
                    case OperatorMask.Field_Copy:
                        decodeCopyIntGenerator(target, TokenBuilder.extractId(token), TypeMask.isOptional(pmapType));
                        break;
                    case OperatorMask.Field_Constant:
                        //this intentionally left blank, does nothing if constant
                        break;
                    case OperatorMask.Field_Default:
                        decodeDefaultIntGenerator(target, TokenBuilder.extractId(token), TypeMask.isOptional(pmapType));
                        break;
                    case OperatorMask.Field_Delta:
                        decodeDeltaIntGenerator(target, TokenBuilder.extractId(token), TypeMask.isOptional(pmapType));
                        break;
                    case OperatorMask.Field_Increment:
                        decodeIncrementIntGenerator(target, TokenBuilder.extractId(token), TypeMask.isOptional(pmapType));
                        break;
                    case OperatorMask.Field_None:
                        target.append("0;//no oper currently not supported.\n");
                        break;
                    default: {
                        target.append("//here as placeholder, this is unsupported\n");
                    }
                }
                target.append(tab + bitMaskName + " = " + bitMaskName + " << " +numShifts + ";\n");
            } //if long, goes to switch to find correct operator to call 
            else if (TypeMask.isLong(pmapType) == true) {
                target.append(tab + "long " + scriptNames[f] + " = ");
                int oper = TokenBuilder.extractOper(token);
                switch (oper) {
                    case OperatorMask.Field_Copy:
                        decodeCopyLongGenerator(target, TokenBuilder.extractId(token), TypeMask.isOptional(pmapType));
                        break;
                    case OperatorMask.Field_Constant:
                        //this intentionally left blank, does nothing if constant
                        break;
                    case OperatorMask.Field_Default:
                        decocdeDefaultLongGenerator(target, TokenBuilder.extractId(token), TypeMask.isOptional(pmapType));
                        break;
                    case OperatorMask.Field_Delta:
                        decodeDeltaLongGenerator(target, TokenBuilder.extractId(token), TypeMask.isOptional(pmapType));
                        break;
                    case OperatorMask.Field_Increment:
                        decodeIncrementLongGenerator(target, TokenBuilder.extractId(token), TypeMask.isOptional(pmapType));
                        break;
                }
                target.append(tab + bitMaskName + " = " + bitMaskName + " << " +numShifts + ";\n");
            } //if string
            else if (TypeMask.isText(pmapType) == true) {
                target.append(tab + "String " + scriptNames[f] + " = ");
                decodeStringGenerator(target, TypeMask.isOptional(pmapType));
                target.append(tab + bitMaskName + " = " + bitMaskName + " << " +numShifts + ";\n");
            } else {
                target.append("Unsupported data type " + pmapType + "\n");
            }
            cursor++;
            argumentList.append(scriptNames[f]);
            if (f != (firstField + fieldCount) - 1) {
                argumentList.append(',');
            }
        }
        target.append("Pipe.releaseReadLock(" + inPipeName + ");\n");
        //open method to call with the variable names
        appendWriteMethodName(target.append(tab), cursor2).append("(");
        target.append(argumentList);
        target.append(");\n");
    }

    /**
     * This method is intentionally left blank so it can overrid the super classes implementation of requestShutDown
     *
     * @param target where the code is written to
     * @throws IOException when the target can not be written to.
     */
    protected void additionalMethods(Appendable target) throws IOException {
    }

    /**
     * This method overrides from the super class, where it is called to add instance variables. This method should not
     * be called from anywhere but the super class.
     *
     * @param target the Appendable target from super class
     * @throws IOException if the target can not be written to
     */
    @Override
    protected void additionalMembers(Appendable target) throws IOException {
        target.append("private long[] " + longDictionaryName + ";\n");
        target.append("private int[] " + intDictionaryName + ";\n");
        target.append("private long[] " + defaultLongDictionaryName + ";\n");
        target.append("private int[] " + defaultIntDictionaryName + ";\n");
        target.append("private Pipe<RawDataSchema> " + inPipeName + ";\n");

    }

    /**
     * writes the code to pull the pmap off of the input pipe
     *
     * @param target where the code is written to.
     */
    protected void decodePmap(Appendable target) {
        try {
            target.append(tab + "long " + mapName + " = ");
            appendStaticCall(target, blobReader, "readPackedLong").append(readerName).append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //incremement int code generator
    protected void decodeIncrementLongGenerator(Appendable target, int index, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodeIncrementLong").append(longDictionaryName).append(", ").append(mapName).append(", ").append(Integer.toString(index)).append(", ").append(bitMaskName).append(", " + isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //default long code generator
    protected void decocdeDefaultLongGenerator(Appendable target, int index, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodeDefaultLong").append(readerName).append(", ").append(mapName).append(", ").append(defaultLongDictionaryName).append(", ").append(bitMaskName).append(", ").append(Integer.toString(index)).append(", " + isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //decode string generator
    protected void decodeStringGenerator(Appendable target, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodeString").append(readerName).append(", " + isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //delta long code generator
    protected void decodeDeltaLongGenerator(Appendable target, int index, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodeDeltaLong").append(longDictionaryName).append(", ").append(readerName).append(", ").append(mapName).append(", ").append(Integer.toString(index)).append(", ").append(bitMaskName).append(", " + isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //default int code generator
    protected void decodeDefaultIntGenerator(Appendable target, int index, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodeDefaultInt").append(readerName).append(", ").append(mapName).append(", ").append(defaultIntDictionaryName).append(", ").append(bitMaskName).append(", ").append(Integer.toString(index)).append(", ").append(isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //delta int code generator
    protected void decodeDeltaIntGenerator(Appendable target, int index, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodeDeltaInt").append(intDictionaryName).append(", ").append(readerName).append(", ").append(mapName).append(", ").append(Integer.toBinaryString(index)).append(", ").append(bitMaskName).append(", " + isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //copy int code generator
    protected void decodeCopyIntGenerator(Appendable target, int index, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodeCopyInt").append(intDictionaryName).append(", ").append(readerName).append(", ").append(mapName).append(", ").append(Integer.toString(index)).append(", ").append(bitMaskName).append(", " + isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //increment int code generator
    protected void decodeIncrementIntGenerator(Appendable target, int index, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodeIncrementInt").append(intDictionaryName).append(", ").append(mapName).append(", ").append(Integer.toString(index)).append(", ").append(bitMaskName).append(", " + isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //int present code generator
    protected void decodePresentIntGenerator(Appendable target, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodePresentInt").append(", ").append(readerName).append(", ").append(mapName).append(", ").append(bitMaskName).append(", " + isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //copy long generator
    protected void decodeCopyLongGenerator(Appendable target, int index, Boolean isOptional) {
        try {
            appendStaticCall(target, decoder, "decodeCopyLong").append(longDictionaryName).append(", ").append(readerName).append(", ").append(mapName).append(", ").append(Integer.toString(index)).append(", ").append(bitMaskName).append(", " + isOptional.toString() + ");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
