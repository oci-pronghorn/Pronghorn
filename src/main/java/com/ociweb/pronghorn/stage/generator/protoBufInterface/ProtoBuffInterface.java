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
        decoderGenerator = new PhastDecoderStageGenerator(schema, decodeTarget, false);
        this.packageName = packageName;
        this.schema = schema;
        this.interfaceTarget = interfaceTarget;
        this.interfaceClassName = interfaceClassName;
    }
    // Interface name change
    // generateNameSpace? Class Name
    // public void Grocery getName()
    // public void Amazon getName()
    // Needs to hold more than one namespace and work at sametime without conflicts
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
     // public boolean Create()
     // TODO: build partial???
    private static void generateCreate(String varName, Appendable target) {
        try {
            target.append(tab +"private static" + " create(){"
                    + "return new buildFirst();"
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
    // cloneName()
    // clones the field
    // check mergeFrom instance.........
    private static void generateClone(Appendable target) {
        try {
            //Clone method generated
            target.append(tab + "public void" + " clone(){"
                    + "\n"
                    + "return create().mergeFrom(buildFirst());"
                    + "\n"
                    + tab +  "}\n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    // addName()
    // add a value
    private static void generateAdd(String varName, String varType, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //Add method generated
             target.append(tab + "public void" + " add" + varNameCamel + "(" + varType + " " + varName
                    + ") {\n"
                    + "\n"
                    + tab +  "}\n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    // Unfinished Stub for List
    private static void generateList(String varName, Appendable target) {
        try {
             //List method generated
             target.append(tab + "public java.util.List<java.lang.Integer>" 
                    // getList() { return varName; }
                    + "get" + varName + "()" 
                    + " {\n"
                    + "return " + varName + "_;"
                    + "\n"
                    + tab +  "}\n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    //public void build()
    //builds the builder
    //TODO :build() stub is empty. 
    // is Build same as buildFirst?
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
    // sets boolean has flags to false
    private static void generateInit(String varName, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //Build method generated
            target.append(tab + "public final boolean" + " isInitialized() {"
                    + "\n");
            // need variable step through loop
            // cycles through has statements to generate false statements
            for(int i = 0; i < 0; i++) {
                target.append(tab + "if (!has" + varNameCamel + "()) {"
                + "\n"
                + "return false;"
                + "\n" + "}" + "\n");
            }
            target.append("}");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    //mergeFrom()
    //Builder?
    //rethink this for clone
    // DELETE NOT NEEDED 
    // REWORK THIS FOR OUR CODE??
    // need help how to implement
    private static void generateMergeFromBuilder(String locationMessage, String messageType, Appendable target) {
         try {
            //mergeFrom method generated
            target.append(tab + "public Builder mergeFrom(" + locationMessage + " other){"
                    + "\n"
                    + "if (other " + messageType + ") {"
                    + "\n"
                    + "return mergeFrom((" + messageType + ")other);"
                    + "\n"
                    + "else {" + "\n"
                    + "super.mergeFrom(other);" + "\n"
                    + "return this;" + "\n" + "}"
                    + "\n"
                    + tab +  "}\n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private static void generateMergeFrom(String locationMessage, String messageType, Appendable target) {
         try {
            //mergeFrom method generated
            target.append(tab + "Good stuff goes here");
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
