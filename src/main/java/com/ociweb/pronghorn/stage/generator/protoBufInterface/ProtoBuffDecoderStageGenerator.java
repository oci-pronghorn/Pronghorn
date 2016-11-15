package com.ociweb.pronghorn.stage.generator.protoBufInterface;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.generator.PhastDecoderStageGenerator;

import java.io.IOException;

/**
 * Created by jake on 11/10/16.
 */
public class ProtoBuffDecoderStageGenerator extends PhastDecoderStageGenerator {
    public ProtoBuffDecoderStageGenerator(MessageSchema schema, Appendable bodyTarget, Boolean isRunnable) {
        super(schema, bodyTarget, isRunnable);
    }

    @Override
    protected void interfaceMembers(Appendable target){
        try {
            target.append("DataInputBlobReader<RawDataSchema> " + readerName +"; \n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void interfaceSetup(Appendable target){
        try {
            target.append("reader = Pipe.inputStream(" + inPipeName + ");\n");
        } catch (IOException e) {
            e.printStackTrace();
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

        //this will keep track of variable names so they can be called as arguments in a later method
        StringBuilder argumentList = new StringBuilder();

        //bitmask goes here
        target.append(tab + "long " + bitMaskName + " = 1;\n");
        target.append(tab + "int position = reader.position();\n");
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
        target.append("Pipe.releasePendingAsReadLock(" + inPipeName + ", " + readerName +".position() - position);\n");
        target.append("Pipe.readNextWithoutReleasingReadLock(" + inPipeName + ");\n");
        //open method to call with the variable names
        appendWriteMethodName(target.append(tab), cursor2).append("(");
        target.append(argumentList);
        target.append(");\n");
    }


}
