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

public class ProtoBuffInterface {

    private PhastDecoderStageGenerator decoderGenerator;
    private PhastEncoderStageGenerator encoderGenerator;
    String interfaceClassName;
    String packageName;
    MessageSchema schema;
    Appendable interfaceTarget;
    private static String tab = "  ";

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
            target.append(tab + "public " + varType + " get" + varNameCamel + "(){\n");
            //return variable, close off, end line.
            target.append(tab + tab +"return " + varName + ";"
                    + "\n"
                    + tab + "}\n");
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
            target.append(tab + "public void" + " set" + varNameCamel + "(" + varType + " " + varName
                    + ") {\n"
                    + tab + tab + "this." + varName + " = " + varName + "; \n"
                    + "} \n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    // public boolean has()
    private static void generateHas(String varName, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //Has method generated
            target.append(tab +"public boolean" + " has" + varNameCamel + "(){"
                    + "\n"
                    + "\n"
                    + tab +  "}\n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    //public void clearName() {  }
    //ressets all fields to default values
    private static void generateClear(String varName, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //Clear method generated
            target.append(tab + "public void" + " clear" + varNameCamel +  "(){"
                    + "\n"
                    + "\n"
                    + tab +  "}\n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    //public void build()
    //builds the builder
    private static void generateBuild(String varName, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //Build method generated
            target.append(tab + "public void" + " build" + varNameCamel +  "(){"
                    + "\n"
                    + "\n"
                    + tab +  "}\n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    // import package
    // ex. import Pronghorn.Grocery.Inventory
    // TODO: Make it work
    // "Working" Stub
    private static void generateImport(String varName, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //import created
            target.append(tab + "import " + varNameCamel +  ""
                    + "\n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    //  main method
    private static void generateMain(Appendable target) {
        try {
            // Empty Main Created
            target.append(tab + "public static void main(String[] args) throws Exception {"
                     + "\n"
                    + "\n"
                    + tab +  "}\n");
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

            //inserts
            while (--i >= 0) {
                int type = TokenBuilder.extractType(tokens[i]);

                if (TypeMask.isLong(type)) {
                    //get
                    generateGetter(scriptNames[i], "long", interfaceTarget);
                    //gas
                    generateHas(scriptNames[i], interfaceTarget);
                    //clear
                    generateClear(scriptNames[i], interfaceTarget);
                } else if (TypeMask.isInt(type)) {
                    //get
                    generateGetter(scriptNames[i], "int", interfaceTarget);
                    //gas
                    generateHas(scriptNames[i], interfaceTarget);
                    //clear
                    generateClear(scriptNames[i], interfaceTarget);
                } else if (TypeMask.isText(type)) {
                    //get
                    generateGetter(scriptNames[i], "String", interfaceTarget);
                    //has
                    generateHas(scriptNames[i], interfaceTarget);
                    //clear
                    generateClear(scriptNames[i], interfaceTarget);
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
