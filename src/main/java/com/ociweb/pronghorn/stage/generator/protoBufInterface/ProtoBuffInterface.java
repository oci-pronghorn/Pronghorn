/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ociweb.pronghorn.stage.generator.protoBufInterface;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.generator.PhastDecoderStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastEncoderStageGenerator;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jake
 */
public class ProtoBuffInterface {

    private PhastDecoderStageGenerator decoderGenerator;
    private PhastEncoderStageGenerator encoderGenerator;
    String interfaceClassName;
    String packageName;
    MessageSchema schema;
    Appendable interfaceTarget;

    public ProtoBuffInterface(String packageName, MessageSchema schema, Appendable decodeTarget, Appendable encodeTarget, Appendable interfaceTarget, String interfaceClassName) {
        encoderGenerator = new PhastEncoderStageGenerator(schema, encodeTarget);
        decoderGenerator = new PhastDecoderStageGenerator(schema, decodeTarget, packageName);
        this.packageName = packageName;
        this.schema = schema;
        this.interfaceTarget = interfaceTarget;
        this.interfaceClassName = interfaceClassName;
    }

    //this method generates a setter given its name, type, and appendable to be placed in.
    private static void generateSetter(String varName, String varType, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //getter method generated
            target.append("public " + varType + " get" + varNameCamel + "(){\n");
            //return variable, close off, end line.
            target.append("return " + varName + ";"
                    + "\n}"
                    + "\n");

        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void buildFirst() {
        try {
            //This is where we declare the class to the output file
            interfaceTarget.append("public class " + interfaceClassName + "{\n");

            //walk through variables and generate has and sets only
            FieldReferenceOffsetManager from = MessageSchema.from(schema);
            int[] tokens = from.tokens;
            String[] scriptNames = from.fieldNameScript;
            long[] scriptIds = from.fieldIdScript;
            int i = tokens.length;

            while (--i >= 0) {
                int type = TokenBuilder.extractType(tokens[i]);

                if (TypeMask.isLong(type)) {
                    //generate setters here
                } else if (TypeMask.isInt(type)) {
                    //and here
                } else if (TypeMask.isText(type)) {
                    //and here
                }
            }

            //closing bracking for class
            interfaceTarget.append("}");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void buildClass() {
        this.buildFirst();
    }
    /* No.
    public class interfaceClassName {
        
    }
     */
}
