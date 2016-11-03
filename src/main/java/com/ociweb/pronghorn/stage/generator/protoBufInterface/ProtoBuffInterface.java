package com.ociweb.pronghorn.stage.generator.protoBufInterface;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.generator.PhastDecoderStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastEncoderStageGenerator;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProtoBuffInterface {
    private PhastDecoderStageGenerator decoderGenerator;
    private PhastEncoderStageGenerator encoderGenerator;
    private String interfaceClassName;
    private String packageName;
    String innerClassName;
    private MessageSchema schema;
    private Appendable interfaceTarget;
    private static String tab = "    ";
    private static String decoderClassName = "DecoderStage";
    private static String encoderClassName = "EncoderStage";
    GraphManager gm;
    FieldReferenceOffsetManager from;

    public ProtoBuffInterface(String packageName, String interfaceClassName, String innerClassName,
                              String filePath, String xmlPath) throws IOException, SAXException, ParserConfigurationException {

        File output = new File(filePath + interfaceClassName + ".java");

        from = TemplateHandler.loadFrom(xmlPath);

        MessageSchema schema = new MessageSchemaDynamic(from);

        PrintWriter target = new PrintWriter(output);
        this.interfaceTarget = target;

        //decode target
        File outputDecode = new File(filePath + decoderClassName + ".java");
        PrintWriter decodeTarget = new PrintWriter(outputDecode);

        //encode target
        File outputEncode = new File(filePath + encoderClassName + ".java");
        PrintWriter encodeTarget = new PrintWriter(outputEncode);

        encoderGenerator = new PhastEncoderStageGenerator(schema, encodeTarget);
        encoderGenerator.processSchema();
        decoderGenerator = new PhastDecoderStageGenerator(schema, decodeTarget, false);
        decoderGenerator.processSchema();
        this.packageName = packageName;
        this.schema = schema;
        this.interfaceClassName = interfaceClassName;
        this.innerClassName = innerClassName;
        this.gm = new GraphManager();

        this.buildClass();
        decodeTarget.close();
        encodeTarget.close();
        target.close();
    }

    private static void generateGetter(String varName, String varType, Appendable target) {
        try {
            //make variable name go to camel case
            String varNameCamel = varName.substring(0, 1).toUpperCase() + varName.substring(1);
            //Getter method generated
            target.append(tab + "public " + varType + " get" + varNameCamel + "(){\n");
            //return variable, close off, end line.
            target.append(tab + tab +"return " + varNameCamel + ";\n"
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
                    + tab + "} \n");
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
                    + tab + tab + "return true;"
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
    public static void generateBuildTypes(String varName, String varType, Appendable target) {
        try {
            for(int i = 0; i < 0; i++) {
                target.append(tab + tab + tab + "Pipe.add" + varType + "(" + varName + ", inPipe); \n");
            }
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void generateNewBuilder() throws IOException {
        //Build method generated
        interfaceTarget.append(tab + "public Builder newbuilder(){"
                // Create Dynamic schema and pipes
                + "\n"
                + tab + tab + "messageSchema = new MessageSchemaDynamic(DecoderStage.FROM);"
                + "\n"
                + tab + tab + "inPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));"
                + "\n"
                + tab + tab + "inPipe.initBuffers();"
                + "\n"
                + tab + tab + "sharedPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance));"
                + "\n"
                + tab +  tab + "sharedPipe.initBuffers();"
                + "\n"
                + tab + tab + "outPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));"
                + "\n"
                + tab + tab + "outPipe.initBuffers();\n"
                + tab + tab + "encStage = new " + encoderClassName + "(gm, inPipe, out);\n"
                + tab + tab + "decStage = new " + decoderClassName + "(gm, );\n");

                interfaceTarget.append( tab + tab + "return new Builder();\n"
                + tab + "}\n");
    }
    //public void build()
    //builds the builder
    private void generateBuild() throws IOException {
        interfaceTarget.append(tab + "public void build(){\n" +
                                tab + tab + "isReady = true;\n" +
                                tab + "}\n");

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

    private void generateRun() throws IOException {
        interfaceTarget.append(tab + "@Override\n" +
                                tab + "public void run(){}\n");

    }

    private void generateStartup() throws IOException {
        interfaceTarget.append(tab + "@Override\n" +
                tab + "public void startup(){}\n");
    }

    private void generateInstanceVariables() throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        int[] tokens = from.tokens;
        String[] scriptNames = from.fieldNameScript;
        long[] scriptIds = from.fieldIdScript;
        int i = tokens.length;

        //from schema
        while (--i >= 0) {
            int type = TokenBuilder.extractType(tokens[i]);
            if (TypeMask.isLong(type)) {
                interfaceTarget.append(tab + "private long " + scriptNames[i] + ";\n");
            } else if (TypeMask.isInt(type)) {
                interfaceTarget.append(tab + "private int " + scriptNames[i] + ";\n");
            } else if (TypeMask.isText(type)) {
                interfaceTarget.append(tab + "private String  " + scriptNames[i] + ";\n");
            }

        }
        interfaceTarget.append(tab + "private MessageSchemaDynamic messageSchema;\n");
        interfaceTarget.append(tab + "private Pipe<MessageSchemaDynamic> inPipe;\n");
        interfaceTarget.append(tab + "private Pipe<RawDataSchema> sharedPipe;\n");
        interfaceTarget.append(tab + "private Pipe<MessageSchemaDynamic> outPipe;\n");
        interfaceTarget.append(tab + "private " + decoderClassName + " decStage;\n");
        interfaceTarget.append(tab + "private " + encoderClassName + " encStage;\n");
        interfaceTarget.append(tab + GraphManager.class.getSimpleName() + "gm;\n");
        from.appendConstuctionSource(interfaceTarget);
        interfaceTarget.append(tab + "private Boolean isReady;\n");
    }
    private void buildFirst() {
        try {
            //This is where we declare the class to the output file
            interfaceTarget.append("public class " + innerClassName + "{\n");
            generateInstanceVariables();
            //make embedded builder class
            interfaceTarget.append(tab + "public static class Builder{\n");
            interfaceTarget.append(tab+tab+"public Builder(){}\n");
            interfaceTarget.append(tab + "}\n");

            generateNewBuilder();

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
                    //test
                    //generateBuildConstr(scriptNames[i], "long", interfaceTarget);
                    //gas
                    generateHas(scriptNames[i], interfaceTarget);
                    //clear
                    generateClear(scriptNames[i], interfaceTarget);
                    generateSetter(scriptNames[i],"long", interfaceTarget);
                } else if (TypeMask.isInt(type)) {
                    //get
                    generateGetter(scriptNames[i], "int", interfaceTarget);
                    //gas
                    generateHas(scriptNames[i], interfaceTarget);
                    //clear
                    generateClear(scriptNames[i], interfaceTarget);
                    generateSetter(scriptNames[i],"int", interfaceTarget);
                } else if (TypeMask.isText(type)) {
                    //get
                    generateGetter(scriptNames[i], "String", interfaceTarget);
                    //has
                    generateHas(scriptNames[i], interfaceTarget);
                    //clear
                    generateClear(scriptNames[i], interfaceTarget);
                    generateSetter(scriptNames[i],"String", interfaceTarget);

                }
                
            }
            //generate build constructor
            generateBuild(interfaceTarget);
            generateStartup();
            generateRun();
            //closing bracking for class
            interfaceTarget.append("}");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void buildOuter() throws IOException {
        interfaceTarget.append("public Class " + interfaceClassName + " extends PronghornStage{\n\n");
    }
    public void buildClass() throws IOException {
        interfaceTarget.append("/*\n");
        interfaceTarget.append("THIS CLASS HAS BEN GENERATED DO NOT MODIFY\n");
        interfaceTarget.append("*/\n");
        interfaceTarget.append("package " + packageName + "\n");
        this.buildOuter();
        this.buildFirst();


        //close outer
        interfaceTarget.append("\n}");


    }
}
