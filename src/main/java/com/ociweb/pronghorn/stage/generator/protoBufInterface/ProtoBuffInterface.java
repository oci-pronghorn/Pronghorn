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
private static void generateGetter(String varName, String varType, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //Getter method generated
            target.append("public " + varType + " get" + varNameCamel + "(){\n");
            //return variable, close off, end line.
            target.append("return " + varName + ";"
                    + "\n}"
                    + "\n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    //public void setName(String name) { this.name = name; }
    private static void generateSetter(String varName, String varType, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //Setter method generated
            target.append("public void" + " set" + varNameCamel + "(" + varType + " " + varName +
                    ") { this." + varName + " = " + varName + "; }; \n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    // public boolean has()
    private static void generateHas(String varName, String varType, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //Has method generated
            target.append("public boolean" + " has" + varNameCamel + "(){\n");
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
            
            //insert has
            interfaceTarget.append("public boolean has").append(scriptNames[i]).append("();\n");
            //inserts
            while (--i >= 0) {
                int type = TokenBuilder.extractType(tokens[i]);

                if (TypeMask.isLong(type)) {
                    //set
                    interfaceTarget.append("public void set").append(scriptNames[i])
                            .append("(").append("Long ").append(scriptNames[i])
                            .append(");\n");
                    //get
                    interfaceTarget.append("public long get").append(scriptNames[i])
                            .append("();\n");
                } else if (TypeMask.isInt(type)) {
                    //set
                    interfaceTarget.append("public void set").append(scriptNames[i])
                            .append("(").append("Int ").append(scriptNames[i])
                            .append(");\n");
                    //get
                    interfaceTarget.append("public int get").append(scriptNames[i])
                            .append("();\n");
                } else if (TypeMask.isText(type)) {
                    //set
                    interfaceTarget.append("public void set").append(scriptNames[i])
                            .append("(").append("String ").append(scriptNames[i])
                            .append(");\n");
                    //get
                    interfaceTarget.append("public String get").append(scriptNames[i])
                            .append("();\n");
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
}
