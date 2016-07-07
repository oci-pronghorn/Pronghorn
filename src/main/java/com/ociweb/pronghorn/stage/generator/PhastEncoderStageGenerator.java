package com.ociweb.pronghorn.stage.generator;

import java.io.IOException;
import com.ociweb.pronghorn.stage.phast.PhastEncoder;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;

import static com.ociweb.pronghorn.util.Appendables.*;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.pipe.token.*;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelReader;
import com.ociweb.pronghorn.util.Appendables;
import java.util.logging.Level;
import javax.swing.text.html.HTML;
import jdk.nashorn.internal.parser.TokenStream;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class PhastEncoderStageGenerator extends TemplateProcessGeneratorLowLevelReader {

    private final Class encoder = PhastEncoder.class;
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
    private final String longValueName = "longVal";
    private final String shortValueName = "shortVal";
    private final String stringValueName = "stringVal";
    private final String tokenName = "token";
    private final String booleanName = "boolean";
    private int count;
    private static final String tab = "    ";
    private static final Logger logger = LoggerFactory.getLogger(PhastEncoderStageGenerator.class);

    public PhastEncoderStageGenerator(MessageSchema schema, Appendable bodyTarget) {
        super(schema, bodyTarget);
        this.bodyTarget = bodyTarget;
    }

    @Override
    protected void additionalImports(MessageSchema schema, Appendable target) {
        try {
            target.append("import ").append(schema.getClass().getCanonicalName()).append(";\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected void headerConstruction() throws IOException {
        bodyTarget.append("package ").append(packageName).append(";\n");
        bodyTarget.append("import ").append(LowLevelStateManager.class.getCanonicalName()).append(";\n");
        bodyTarget.append("import ").append(Pipe.class.getCanonicalName()).append(";\n");
        bodyTarget.append("import ").append(FieldReferenceOffsetManager.class.getCanonicalName()).append(";\n");
        bodyTarget.append("import ").append(Appendables.class.getCanonicalName()).append(";\n");
        bodyTarget.append("import ").append(MessageSchemaDynamic.class.getCanonicalName()).append(";\n");
        additionalImports(schema, bodyTarget);
        
        bodyTarget.append("public class ").append(className).append(" implements Runnable {\n");

        bodyTarget.append("\n");
        bodyTarget.append("private void requestShutdown() {};\n"); //only here so generated code passes compile.
    }

    @Override
    public void processSchema() throws IOException {
        headerConstruction();

        defineMembers();
        additionalTokens(bodyTarget);

        final FieldReferenceOffsetManager from = MessageSchema.from(schema);

        //Build top level entry point
        processCallerPrep();
        for (int cursor = 0; cursor < from.fragScriptSize.length; cursor++) {
            boolean isFragmentStart = 0 != from.fragScriptSize[cursor];
            if (isFragmentStart) {
                processCaller(cursor);
            }

        }
        processCallerPost();

        //Build fragment consumption methods
        for (int cursor = 0; cursor < from.fragScriptSize.length; cursor++) {
            boolean isFragmentStart = 0 != from.fragScriptSize[cursor];

            if (isFragmentStart) {
                processCalleeOpen(cursor);

                boolean isMessageStart = FieldReferenceOffsetManager.isTemplateStart(from, cursor);

                if (isMessageStart) {
                    processFragment(1, cursor, from);
                } else {
                    processFragment(0, cursor, from);
                }

                processCalleeClose(cursor);
            }
        }

        footerConstruction();
    }

    // Additional Token method to append any longs, ins or string variables
    protected void additionalTokens(Appendable target) throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        int[] tokens = from.tokens;
        int[] intDict = from.newIntDefaultsDictionary();
        String[] scriptNames = from.fieldNameScript;
        long[] scriptIds = from.fieldIdScript;
        long[] longDict = from.newLongDefaultsDictionary();
        int i = tokens.length;

        while (--i >= 0) {
            int type = TokenBuilder.extractType(tokens[i]);

            if (TypeMask.isLong(type)) {
                target.append("private long ").append(scriptNames[i]).append(";\n");
                count++;
            } else if (TypeMask.isInt(type)) {
                target.append("private int ").append(scriptNames[i]).append(";\n");
                count++;
            } else if (TypeMask.isText(type)) {
                target.append("private String ").append(scriptNames[i]).append(";\n");
                count++;
            }
        }
    }

    //  BuilderInt Factory
    protected void encodePmapBuilderInt(MessageSchema schema, Appendable target, int token, int index, String valName) {
        try {
            appendStaticCall(target, encoder, "pmapBuilderInt")
                    .append(pmapName).append(", ")
                    .append(Integer.toString(token)).append(", ")
                    .append(valName).append(", ")
                    .append(intDictionaryName + "[" + index + "]").append(", ")
                    .append(defIntDictionaryName + "[" + index + "]").append(", ")
                    .append("(" + valName + " == null)")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // builderLong Factory
    protected void encodePmapBuilderLong(MessageSchema schema, Appendable target, int token, int index, String valName) {
        try {
            appendStaticCall(target, encoder, "pmapBuilderLong")
                    .append(pmapName).append(", ")
                    .append(Integer.toString(token)).append(", ")
                    .append(valName).append(", ")
                    .append(longDictionaryName + "[" + index + "]").append(", ")
                    .append(defLongDictionaryName + "[" + index + "]").append(", ")
                    .append("(" + valName + " == null)")
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
    protected void encodeIntPresentGenerator(MessageSchema schema, Appendable target) {
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
    protected void encodeDeltaIntGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDeltaInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DeltaLong Factory
    protected void encodeDeltaLongGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDeltaLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(indexName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(longValueName)
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
                    .append(indexName)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IncrementInt Factory
    protected void incrementIntGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "incrementInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CopyInt Factory
    protected void copyIntGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "copyInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DefaultInt Factory
    protected void encodeDefaultIntGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDefaultInt")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(intValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // LongPresent Factory
    protected void encodeLongPresentGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeLongPresentGenerator")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(longValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IncrementLong Factory
    protected void incrementLongGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "incrementLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CopyLong Factory
    protected void copyLongGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "copyLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DefaultLong Factory
    protected void encodeDefaultLongGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDefaultLong")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(longValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ShortPresent Factory
    protected void encodeShortPresentGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeShortPresent")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName)
                    .append(shortValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // IncrementShort Factory
    protected void incrementShortGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "incrementShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CopyShort Factory
    protected void copyShortGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "copyShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DefaultShort Factory
    protected void encodeDefaultShortGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDefaultShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(shortValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // DeltaShort Factory
    protected void encodeDeltaShortGenerator(MessageSchema schema, Appendable target) {
        try {
            target.append(tab);
            appendStaticCall(target, encoder, "encodeDeltaShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(indexName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(shortValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // BodyBuilder OverRide. Lots of Good stuff goes here
    // Creates Pmap for encoding
    @Override
    protected void bodyBuilder(MessageSchema schema, int cursor, int fragmentParaCount, CharSequence[] fragmentParaTypes, CharSequence[] fragmentParaArgs, CharSequence[] fragmentParaSuff) {

        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        cursor++;
        int curCursor = cursor;
        int curCursor2 = cursor;
        boolean pmapOptional = false;

        //tracking id of the pmap so we can give it to the phast encoder later
        try {
            bodyTarget.append(tab + "long " + indexName + " = 1;\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //traverse all tokens and print out a pmap builder for each of them
        for (int paramIdx = 0; paramIdx < fragmentParaCount; paramIdx++) {
            int token = from.tokens[curCursor];
            int pmapType = TokenBuilder.extractType(token);

            String varName = new StringBuilder().append(fragmentParaArgs[paramIdx]).append(fragmentParaSuff[paramIdx]).toString();
            String varType = new StringBuilder().append(fragmentParaTypes[paramIdx]).toString();

            String isNull = "false";

            //TODO: to hex string isntead of string
            if (TypeMask.isOptional(pmapType)) {
                isNull = "true";
            }

            if (varType.equals("int")) {
                try {
                    bodyTarget.append(tab + "activePmap = ");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                encodePmapBuilderInt(schema, bodyTarget, token, paramIdx, varName);

            } else if (varType.equals("long")) {
                try {
                    bodyTarget.append(tab + "activePmap = ");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                encodePmapBuilderLong(schema, bodyTarget, token, paramIdx, varName);

            } else if (varType.equals("StringBuilder")) {
                try {
                    bodyTarget.append(tab + "activePmap = ");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                encodePmapBuilderString(schema, bodyTarget, token, varName);

            } else {
                try {
                    bodyTarget.append("caught by nothing\n");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            if (paramIdx != 0) {

                try {
                    bodyTarget.append(tab + indexName + " = " + indexName + " << 1;\n");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            curCursor += TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];
        }

        //line break for readability
        try {
            bodyTarget.append("\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //traverses all data and pulls them off the pipe
        for (int paramIdx = 0; paramIdx < fragmentParaCount; paramIdx++) {
            int token = from.tokens[curCursor2];
            int pmapType = TokenBuilder.extractType(token);
            String varName = new StringBuilder().append(fragmentParaArgs[paramIdx]).append(fragmentParaSuff[paramIdx]).toString();
            String varType = new StringBuilder().append(fragmentParaTypes[paramIdx]).toString();
            if (TypeMask.isInt(pmapType) == true) {
                int oper = TokenBuilder.extractOper(token);
                switch (oper) {
                    case OperatorMask.Field_Copy:
                        copyIntGenerator(schema, bodyTarget);
                        break;
                    case OperatorMask.Field_Constant:
                        //this intentionally left blank, does nothing if constant
                        break;
                    case OperatorMask.Field_Default:
                        encodeDefaultIntGenerator(schema, bodyTarget);
                        break;
                    case OperatorMask.Field_Delta:
                        encodeDeltaIntGenerator(schema, bodyTarget);
                        break;
                    case OperatorMask.Field_Increment:
                        incrementIntGenerator(schema, bodyTarget);
                        break;
                    default: {
                        try {
                            bodyTarget.append("Unsupported Operator Type");
                        } catch (IOException ex) {
                            java.util.logging.Logger.getLogger(PhastEncoderStageGenerator.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            } else if (TypeMask.isLong(pmapType) == true) {
                int oper = TokenBuilder.extractOper(token);
                switch (oper) {
                    case OperatorMask.Field_Copy:
                        copyLongGenerator(schema, bodyTarget);
                        break;
                    case OperatorMask.Field_Constant:
                        //this intentionally left blank, does nothing if constant
                        break;
                    case OperatorMask.Field_Default:
                        encodeDefaultLongGenerator(schema, bodyTarget);
                        break;
                    case OperatorMask.Field_Delta:
                        encodeDeltaLongGenerator(schema, bodyTarget);
                        break;
                    case OperatorMask.Field_Increment:
                        incrementLongGenerator(schema, bodyTarget);
                        break;
                }
            } //else if string
            
            else if (TypeMask.isText(pmapType)== true) {
                encodeStringGenerator(schema, bodyTarget, varName);
            }
            
            else {
                try {
                    bodyTarget.append("Unsupported data type " + pmapType + "\n");
                } catch (IOException ex) {
                    logger.error("Trying to write to body of code code = " + ex);
                }
            }
            curCursor2 += TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];
        }

    }
}
