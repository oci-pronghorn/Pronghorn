package com.ociweb.pronghorn.stage.generator;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;

import java.io.IOException;

import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.phast.PhastEncoder;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;

import static com.ociweb.pronghorn.util.Appendables.*;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.token.*;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelReader;

import java.lang.reflect.Type;
import java.nio.channels.Pipe;
import java.util.Stack;
import java.util.logging.Level;

import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class PhastEncoderStageGenerator extends TemplateProcessGeneratorLowLevelReader {

    private final Class encoder = PhastEncoder.class;
    private final Class blobWriter = DataOutputBlobWriter.class;
    private final Appendable bodyTarget;
    private final String defLongDictionaryName = "defLongDictionary";
    private final String defIntDictionaryName = "defIntDictionary";
    private final String defShortDictionaryName = "defShortDictionary";
    //short not supported yet
    //private final String defShortDictionaryName = "defShortDictionary";
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

    public PhastEncoderStageGenerator(MessageSchema schema, Appendable bodyTarget) {
        super(schema, bodyTarget, generateClassName(schema) + " extends PronghornStage",
                generateClassName(schema), schema.getClass().getPackage().getName() + ".build");
        this.bodyTarget = bodyTarget;
        generateRunnable = false;
    }

    /**
     * This method generates a class name based on the input schema
     *
     * @param schema This is an object created from an XML schema file
     */
    private static String generateClassName(MessageSchema schema) {
        if (schema instanceof MessageSchemaDynamic) {
            String name = MessageSchema.from(schema).name.replaceAll("/", "").replaceAll(".xml", "") + "EncoderStage";
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
            target.append("import " + PhastEncoder.class.getCanonicalName() + ";\n");
            target.append("import " + DataOutputBlobWriter.class.getCanonicalName() + ";\n");
            target.append("import com.ociweb.pronghorn.pipe.*;\n");
            target.append("import " + PronghornStage.class.getCanonicalName() + ";\n");
            interfaceImports(target);
            target.append("\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void interfaceImports(Appendable target){

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
        target.append("long[] " + longDictionaryName + ";\n");
        target.append("int[] " + intDictionaryName + ";\n");
        target.append("long[] " + defLongDictionaryName + ";\n");
        target.append("int[] " + defIntDictionaryName + ";\n");
        //isntantiate pipe
        target.append("DataOutputBlobWriter<" + schema.getClass().getSimpleName() + "> " + writerName + ";\n");
        //target.append("private Pipe<MessageSchemaDynamic> " + inPipeName + ";\n");
        target.append("private Pipe<RawDataSchema> " + outPipeName + ";\n");
        interfaceMembers(target);
    }

    protected void interfaceMembers(Appendable target){

    }

    /**
     * This method generates variables necessary to begin a message
     *
     * @param target The target that the code is being written to
     * @throws IOException if the target can not be written to
     */
    private void generateVariables(Appendable target) throws IOException {
        target.append(tab + "long " + pmapName + " = 0;\n");
        bodyTarget.append(tab + "DataOutputBlobWriter<RawDataSchema> " + writerName + " = Pipe.outputStream(" +
                outPipeName + ");\n");
        bodyTarget.append(tab + "DataOutputBlobWriter.openField(" + writerName + ");\n");

    }

    /**
     * This method is to be overridden and called on by the super class. It should not be called anywhere but by the
     * super class
     *
     * @param target The target that the code is being written to
     */
    @Override
    protected void additionalLoopLogic(Appendable target) {
        try {
            target.append("  && Pipe.hasRoomForWrite(" + outPipeName + ")");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method is to be overridden and called on by the super class. It should not be called anywhere but by the
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

            target.append("Pipe<MessageSchemaDynamic>  " + inPipeName + ", ");
            target.append("Pipe<RawDataSchema> " + outPipeName );
            additionalArgs(target);
            target.append(") {\n");

            target.append(tab).append("super(gm," + inPipeName + ",").append(outPipeName).append(");\n");
            target.append(tab).append("this." + outPipeName + " = " + outPipeName + ";\n");
            target.append(tab + "this." + inPipeName + " = " + inPipeName + ";\n");
            target.append(tab);
            Appendables.appendStaticCall(target, Pipe.class, "from").append(inPipeName)
                    .append(").validateGUID(FROM_GUID);\n");
        }
        target.append(tab + intDictionaryName + " = FROM.newIntDefaultsDictionary();\n");
        target.append(tab + longDictionaryName + " = FROM.newLongDefaultsDictionary();\n");
        target.append(tab + defIntDictionaryName + " = FROM.newIntDefaultsDictionary();\n");
        target.append(tab + defLongDictionaryName + " = FROM.newLongDefaultsDictionary();\n");
        additionalConstructorLogic(target);
        target.append("}\n\n");
    }

    protected void additionalConstructorLogic(Appendable target){

    }

    protected void additionalArgs(Appendable target){

    }
    /**
     * This method overrides its super method to add the proper logic in case of a -1
     * being written to the Pipe
     *
     * @param target where the code is being written in super class
     * @throws IOException when target can not be written to
     */
    @Override
    protected void negativeOneCase(Appendable target) throws IOException {
        target.append(tab+tab).append("case -1:\n");

        target.append(tab+tab+tab).append("Pipe.confirmLowLevelRead(" + inPipeName + ", Pipe.EOF_SIZE);\n");
        target.append(tab+tab+tab).append("Pipe.publishEOF(" + outPipeName + ");\n");
        target.append(tab+tab+tab).append("requestShutdown();\n");
    }

    /**
     * This method is overridden and left blank on purpose. It overrides an empty request shut down, and writes nothing
     * so the class may use the request shut down from PronghornStage
     *
     * @throws IOException when target can not be written to
     */
    @Override
    protected void generateRequestShutDown() throws IOException {
    }

    /**
     * The body of the class is generated from this method. It is called from the super class, and should only be called
     * from the super class.
     *
     * @param schema            an object that has loaded in the correct XML file
     * @param cursor            provided by the super class, so we know where the message begins
     * @param fragmentParaCount the amount of variables we are are using for the message
     * @param fragmentParaTypes this array holds the types that the variables are
     * @param fragmentParaArgs  this array holds the parameters data
     * @param fragmentParaSuff  this array holds the name of the variables
     */
    @Override
    protected void bodyBuilder(MessageSchema schema, int cursor, int fragmentParaCount,
                               CharSequence[] fragmentParaTypes, CharSequence[] fragmentParaArgs,
                               CharSequence[] fragmentParaSuff) {

        //create FROM which is generated from the schema provided.
        FieldReferenceOffsetManager from = MessageSchema.from(schema);

        //increment to pass over the group start at the begging of array
        cursor++;
        //make two cursors, one for pmap building and other for encoding
        int curCursor = cursor;
        int curCursor2 = cursor;

        // this only used if supporting preamble : int bitMask = from.templateOffset;

        //If the appendable can not be written to, throw an error
        try {

            //call to instantiate dictionaries
            generateVariables(bodyTarget);
            printPmapBuilding(fragmentParaCount, fragmentParaArgs, fragmentParaSuff, fragmentParaTypes, curCursor, from);

            //taking in pmap
            bodyTarget.append(tab + "DataOutputBlobWriter.writePackedLong(" + writerName + ", " + pmapName + ");\n");
            //instantiating bitmask
            bodyTarget.append(tab + "long " + bitMaskName + " = 1;\n\n");

            //traverses all parameters in message and pulls them off the pipe
            for (int paramIdx = 0; paramIdx < fragmentParaCount; paramIdx++) {
                int token = from.tokens[curCursor2];
                int pmapType = TokenBuilder.extractType(token);
                String varName = new StringBuilder().append(fragmentParaArgs[paramIdx]).append(fragmentParaSuff[paramIdx]).toString();
                String varType = new StringBuilder().append(fragmentParaTypes[paramIdx]).toString();
                //reset numshifts
                int numShifts = 0;
                if (TypeMask.isOptional(pmapType)){
                    numShifts++;
                }
                //if int, goes to switch to find correct operator to call
                if (TypeMask.isInt(pmapType) == true) {
                    int oper = TokenBuilder.extractOper(token);
                    numShifts++;
                    switch (oper) {
                        case OperatorMask.Field_Copy:
                            copyIntGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName, TypeMask.isOptional(pmapType));
                            break;
                        case OperatorMask.Field_Constant:
                            //this intentionally left blank, does nothing if constant
                            break;
                        case OperatorMask.Field_Default:
                            encodeDefaultIntGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName, TypeMask.isOptional(pmapType));
                            break;
                        case OperatorMask.Field_Delta:
                            encodeDeltaIntGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName, TypeMask.isOptional(pmapType));
                            break;
                        case OperatorMask.Field_Increment:
                            incrementIntGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName, TypeMask.isOptional(pmapType));
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
                            copyLongGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName, TypeMask.isOptional(pmapType));
                            break;
                        case OperatorMask.Field_Constant:
                            //this intentionally left blank, does nothing if constant
                            break;
                        case OperatorMask.Field_Default:
                            encodeDefaultLongGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName, TypeMask.isOptional(pmapType));
                            break;
                        case OperatorMask.Field_Delta:
                            encodeDeltaLongGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName, TypeMask.isOptional(pmapType));
                            break;
                        case OperatorMask.Field_Increment:
                            incrementLongGenerator(schema, bodyTarget, TokenBuilder.extractId(token), varName, TypeMask.isOptional(pmapType));
                            break;
                    }
                } //if string
                else if (TypeMask.isText(pmapType) == true) {
                    numShifts = 1;
                    encodeStringGenerator(schema, bodyTarget, varName, TypeMask.isOptional(pmapType));
                } else {
                    bodyTarget.append("Unsupported data type " + pmapType + "\n");
                }
                if (paramIdx != (fragmentParaCount - 1)) {
                    bodyTarget.append(tab + bitMaskName + " = " + bitMaskName + " << " + numShifts + ";\n");
                }
                curCursor2 += TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];
            }
            bodyTarget.append(tab + "DataOutputBlobWriter.closeLowLevelField(" + writerName + ");\n");
            bodyTarget.append(tab + "Pipe.publishWrites(" + outPipeName + ");\n");
            additionalBody(bodyTarget);

        } catch (IOException e) {
            java.util.logging.Logger.getLogger(PhastEncoderStageGenerator.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    protected void additionalBody(Appendable target){

    }

    /**
     * This method will print out code to call methods in PhastEncoder, to construct the pmap
     *
     * @param fragmentParaCount the number of parametets
     * @param fragmentParaArgs  holds the name
     * @param fragmentParaSuff  also holds the name
     * @param fragmentParaTypes the types that the parameters are
     * @param curCursor         where to start
     * @param from              the FROM generated from the given schema
     * @throws IOException when it can not write to the appendable
     */
    private void printPmapBuilding(int fragmentParaCount, CharSequence[] fragmentParaArgs,
                                   CharSequence[] fragmentParaSuff, CharSequence[] fragmentParaTypes,
                                   int curCursor, FieldReferenceOffsetManager from) throws IOException {
        /*
        Make a stack for the tokens, so they can in reverse order. You need them in reverse so the least
        significant bit, is the first item that comes through.
        */
        Stack<Integer> tokens = new Stack<>();
        for (int paramIdx = 0; paramIdx < fragmentParaCount; paramIdx++) {
            int token = from.tokens[curCursor];
            tokens.push(token);
            curCursor += TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];
        }

        //traverse all tokens and print out a pmap builder for each of them
        int i = fragmentParaCount - 1;
        while (i >= 0) {
            int token = tokens.pop();
            int pmapType = TokenBuilder.extractType(token);

            String varName = new StringBuilder().append(fragmentParaArgs[i]).append(fragmentParaSuff[i]).toString();
            String varType = new StringBuilder().append(fragmentParaTypes[i]).toString();

            String isNull = "false";

            if (TypeMask.isOptional(pmapType)) {
                isNull = "true";
            }
            //call appropriate pmap builder according to type
            if (varType.equals("int")) {
                bodyTarget.append(tab + pmapName + " = ");
                encodePmapBuilderInt(schema, bodyTarget, token, TokenBuilder.extractId(token), varName, isNull);

            } else if (varType.equals("long")) {
                bodyTarget.append(tab + pmapName + " = ");
                encodePmapBuilderLong(schema, bodyTarget, token, TokenBuilder.extractId(token), varName, isNull);

            } else if (varType.equals("StringBuilder")) {
                bodyTarget.append(tab + pmapName + " = ");
                encodePmapBuilderString(schema, bodyTarget, token, varName);

            } else {
                bodyTarget.append("caught by nothing\n");
            }

            curCursor += TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];
            i--;
        }
    }

    //  BuilderInt Factory
    protected void encodePmapBuilderInt(MessageSchema schema, Appendable target, int token, int index, String valName, String isNull) {
        try {
            //TODO: add support for isnull
            appendStaticCall(target, encoder, "pmapBuilderInt")
                    .append(pmapName).append(", ")
                    .append(Integer.toString(TokenBuilder.extractType(token))).append(", ")
                    .append(Integer.toString(TokenBuilder.extractOper(token))).append(", ")
                    .append(valName).append(", ")
                    .append(intDictionaryName + "[" + index + "]").append(", ")
                    .append(defIntDictionaryName + "[" + index + "]").append(", ")
                    .append(isNull)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // builderLong Factory
    protected void encodePmapBuilderLong(MessageSchema schema, Appendable target, int token, int index, String valName, String isNull) {
        try {
            appendStaticCall(target, encoder, "pmapBuilderLong")
                    .append(pmapName).append(", ")
                    .append(Integer.toString(TokenBuilder.extractType(token))).append(", ")
                    .append(Integer.toString(TokenBuilder.extractOper(token))).append(", ")
                    .append(valName).append(", ")
                    .append(longDictionaryName + "[" + index + "]").append(", ")
                    .append(defLongDictionaryName + "[" + index + "]").append(", ")
                    .append(isNull)
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
                    .append(Integer.toString(token)).append(", ")
                    .append("(" + valName + " == null)")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IntPresent Factory
    protected void encodeIntPresentGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeIntPresent")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(intValueName).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DeltaInt Factory
    protected void encodeDeltaIntGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDeltaInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DeltaLong Factory
    protected void encodeDeltaLongGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDeltaLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // String Factory
    protected void encodeStringGenerator(MessageSchema schema, Appendable target, String valName, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeString")
                    .append(writerName).append(", ")
                    .append(valName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IncrementInt Factory
    protected void incrementIntGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "incrementInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CopyInt Factory
    protected void copyIntGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "copyInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DefaultInt Factory
    protected void encodeDefaultIntGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDefaultInt")
                    .append(defIntDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // LongPresent Factory
    protected void encodeLongPresentGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeLongPresentGenerator")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IncrementLong Factory
    protected void incrementLongGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "incrementLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CopyLong Factory
    protected void copyLongGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "copyLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DefaultLong Factory
    protected void encodeDefaultLongGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDefaultLong")
                    .append(defLongDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ShortPresent Factory
    protected void encodeShortPresentGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeShortPresent")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName)
                    .append(value).append(", ").append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IncrementShort Factory
    protected void incrementShortGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "incrementShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CopyShort Factory
    protected void copyShortGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "copyShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DefaultShort Factory
    protected void encodeDefaultShortGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDefaultShort")
                    .append(defShortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DeltaShort Factory
    protected void encodeDeltaShortGenerator(MessageSchema schema, Appendable target, int idx, String value, Boolean isOptional) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDeltaShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(Integer.toString(idx)).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(value).append(", ")
                    .append(isOptional.toString())
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
