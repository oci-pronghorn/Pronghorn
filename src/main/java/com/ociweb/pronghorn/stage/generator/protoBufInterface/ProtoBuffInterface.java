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
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProtoBuffInterface {
    private ProtoBuffDecoderStageGenerator decoderGenerator;
    private ProtoBuffEncoderStageGenerator encoderGenerator;
    private String interfaceClassName;
    private String packageName;
    String innerClassName;
    private MessageSchema schema;
    private Appendable interfaceTarget;

    private static String tab = "    ";
    private String decoderClassName;
    private String decoderInstanceName = "dec";
    private String encoderClassName;
    private String encoderInstanceName = "enc";
    private String schedulerName = "scheduler";
    private String isWritingName = "isWriting";
    private String inPipeName = "inPipe";
    private String outPipeName = "outPipe";
    private String sharedPipeName = "transmittedPipe";
    private String inputStreamName = "in";
    private String outputStreamName = "out";
    private String gmName = "gm";
    private String messageSchemaName = "messageSchema";
    private String fromName = "FROM";

    GraphManager gm;
    FieldReferenceOffsetManager from;


    public ProtoBuffInterface(String packageName, String interfaceClassName, String innerClassName,
                              String filePath, String xmlPath) throws IOException, SAXException, ParserConfigurationException {


        File output = new File(filePath + interfaceClassName + ".java");

        from = TemplateHandler.loadFrom(xmlPath);

        MessageSchema schema = new MessageSchemaDynamic(from);

        decoderClassName = generateClassNameDecoder(schema);
        encoderClassName = generateClassNameEncoder(schema);

        PrintWriter target = new PrintWriter(output);
        this.interfaceTarget = target;

        //decode target
        File outputDecode = new File(filePath + decoderClassName + ".java");
        PrintWriter decodeTarget = new PrintWriter(outputDecode);

        //encode target
        File outputEncode = new File(filePath + encoderClassName + ".java");
        PrintWriter encodeTarget = new PrintWriter(outputEncode);

        encoderGenerator = new ProtoBuffEncoderStageGenerator(schema, encodeTarget);
        encoderGenerator.processSchema();
        decoderGenerator = new ProtoBuffDecoderStageGenerator(schema, decodeTarget, false);
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
            target.append(tab + tab + "public " + varType + " get" + varNameCamel + "(){\n");
            //return variable, close off, end line.
            if (varType == "int")
                target.append(tab + tab + tab + "return PipeReader.readInt(query.inPipe, query." + varName + "loc); \n");
            if (varType == "long")
                target.append(tab + tab + tab + "return PipeReader.readLong(query.inPipe, query." + varName + "loc); \n");
            if (varType == "String") {
                target.append(tab + tab + tab + "StringBuilder str = new StringBuilder();\n");
                target.append(tab + tab + tab + "PipeReader.readASCII(query.inPipe, query." + varName + "loc, str); \n");
                target.append(tab + tab + tab + "return str.toString(); \n");
            }

            target.append(tab + tab + tab + "}\n");
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
            target.append(tab + tab + tab + "public void" + " set" + varNameCamel + "(" + varType + " " + varName
                    + ") {\n");
            target.append(
                    "                if (!query.isPrimed){\n" +
                            "                    PipeWriter.tryWriteFragment(inPipe, 0);\n" +
                            "                    query.isPrimed = true;\n" +
                            "                }\n");
            if (varType == "int")
                target.append( tab + tab + tab + tab + "PipeWriter.writeInt(query.inPipe, query." + varName + "loc, " + varName + "); \n");
            if (varType == "long")
                target.append( tab + tab + tab + tab + "PipeWriter.writeLong(query.inPipe, query." + varName + "loc, " + varName + "); \n");
            if (varType == "String")
                target.append( tab + tab + tab + tab + "PipeWriter.writeASCII(query.inPipe, query." + varName + "loc, " + varName + "); \n");

            target.append( tab + tab + tab + "} \n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void generateLOC(String messageName) throws IOException {
        int[] tokens = from.tokens;
        String[] scriptNames = from.fieldNameScript;
        long[] scriptIds = from.fieldIdScript;
        int i = tokens.length;

        //from schema
        while (--i >= 0) {
            int type = TokenBuilder.extractType(tokens[i]);
            if (TypeMask.isLong(type)) {
                interfaceTarget.append(tab + tab + "private final int " + scriptNames[i] + "loc = FROM.getLoc(\"" + messageName + "\", \"" + scriptNames[i] + "\");\n");
            } else if (TypeMask.isInt(type)) {
                interfaceTarget.append(tab + tab + "private final int " + scriptNames[i] + "loc = FROM.getLoc(\"" + messageName + "\", \"" + scriptNames[i] + "\");\n");
            } else if (TypeMask.isText(type)) {
                interfaceTarget.append(tab + tab + "private final int " + scriptNames[i] + "loc = FROM.getLoc(\"" + messageName + "\", \"" + scriptNames[i] + "\");\n");
            }
        }
    }

    private void additionalInstanceVariables(){
        try {
            interfaceTarget.append(
                    "        private GraphManager gm;\n" +
                    "        private Pipe<MessageSchemaDynamic> inPipe;\n" +
                    "        private Pipe<MessageSchemaDynamic> outPipe;\n" +
                    "        private Pipe<RawDataSchema> transmittedPipe;\n" +
                    "        private GroceryExampleEncoderStage enc;\n" +
                    "        private GroceryExampleDecoderStage dec;\n" +
                    "        private boolean isWriting;\n" +
                    "        InputStream in;\n" +
                    "        OutputStream out;\n" +
                    "        ThreadPerStageScheduler scheduler;\n" +
                    "        Boolean isPrimed;\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    private void generateSetters(){
        int[] tokens = from.tokens;
        String[] scriptNames = from.fieldNameScript;
        long[] scriptIds = from.fieldIdScript;
        int i = tokens.length;

        //from schema
        while (--i >= 0) {
            int type = TokenBuilder.extractType(tokens[i]);
            if (TypeMask.isLong(type)) {
                generateSetter(scriptNames[i],"long", interfaceTarget);
            } else if (TypeMask.isInt(type)) {
                generateSetter(scriptNames[i],"int", interfaceTarget);
            } else if (TypeMask.isText(type)) {
                generateSetter(scriptNames[i],"String", interfaceTarget);
            }

        }
    }

    private void generateGetters(){
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        int[] tokens = from.tokens;
        String[] scriptNames = from.fieldNameScript;
        long[] scriptIds = from.fieldIdScript;
        int i = tokens.length;

        //from schema
        while (--i >= 0) {
            int type = TokenBuilder.extractType(tokens[i]);
            if (TypeMask.isLong(type)) {
                generateGetter(scriptNames[i], "long", interfaceTarget);
            } else if (TypeMask.isInt(type)) {
                generateGetter(scriptNames[i], "int", interfaceTarget);
            } else if (TypeMask.isText(type)) {
                generateGetter(scriptNames[i], "String", interfaceTarget);
            }

        }
    }

    public void buildClass() throws IOException {
        interfaceTarget.append("/*\n");
        interfaceTarget.append("THIS CLASS HAS BEEN GENERATED DO NOT MODIFY\n");
        interfaceTarget.append("*/\n");
        interfaceTarget.append("package "+ packageName + ";\n");
        generateImports(interfaceTarget);
        generateClassDeclaration(interfaceTarget, interfaceClassName);
        from.appendConstuctionSource(interfaceTarget);
        generateLOC(innerClassName);
        additionalInstanceVariables();

        generateRun(interfaceTarget);
        generate3ArgConstructor(interfaceTarget);
        generate2ArgConstructor(interfaceTarget);

        generateInnerClass(interfaceTarget, innerClassName);
        interfaceTarget.append("}\n");
    }

    private void generateClassDeclaration(Appendable target, String className) throws IOException{
        target.append("public class ").append(className).append(" extends PronghornStage{\n");
    }


    private void generateInnerClass(Appendable target, String className) throws IOException{
        target.append(tab).append("public class ").append(className).append("{\n");
        target.append(tab+tab).append("private ").append(interfaceClassName).append(" query;\n");
        generateGetters();
        generateAdditionalMethods(interfaceTarget);
        generateBuilderClass(interfaceTarget);
        target.append(tab + "}\n");
    }

    private void generateBuilderClass(Appendable target) throws IOException{
        target.append(tab+tab).append("public class Builder{\n")
                .append(tab+tab+tab).append("private ").append(innerClassName).append(" messages;\n")
                .append(tab+tab+tab).append("private ").append(interfaceClassName).append(" query;\n");
        generateBuilderConstructor(target);
        target.append(tab + tab + "}\n");
    }

    private void generateBuilderConstructor(Appendable target) throws IOException{
        target.append(tab+tab+tab).append("private Builder(){\n")
                .append(tab+tab+tab+tab).append("GraphManager ").append(gmName).append("= new GraphManager();\n")
                .append(tab+tab+tab+tab).append("Pipe<RawDataSchema> ").append(sharedPipeName)
                .append(" = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance));\n")
                .append(tab+tab+tab+tab).append("query = new ").append(interfaceClassName).append("(true, ")
                .append(gmName).append(", " + sharedPipeName + ");\n")
                .append(tab+tab+tab).append("}\n\n");

        generateSetters();
        generateBuild(target);
    }

    private void generateRun(Appendable target) throws IOException{
        target.append(tab + "public void run(){\n");
        target.append(tab+tab).append("if(" + isWritingName + "){\n");
        target.append(tab+tab+tab).append("while(Pipe.contentRemaining(" + sharedPipeName + ") > 0){\n")
                .append(tab+tab+tab+tab).append("try{\n")
                .append(tab+tab+tab+tab+tab).append("Pipe.writeFieldToOutputStream(" + sharedPipeName + ", " + outputStreamName +");\n")
                .append(tab+tab+tab+tab).append("} catch (IOException e) {\n")
                .append(tab+tab+tab+tab+tab).append("e.printStackTrace();\n")
                .append(tab+tab+tab+tab).append("}\n")
                .append(tab+tab+tab).append("}\n")
                .append(tab+tab).append("} else{\n")
                .append(tab+tab+tab).append("try{\n")
                .append(tab+tab+tab+tab).append("while(" + inputStreamName + ".available() > 0 && Pipe.contentRemaining(" + sharedPipeName + ") > 0){\n")
                .append(tab+tab+tab+tab+tab).append("Pipe.readFieldFromInputStream(").append(sharedPipeName + ", ").append(inputStreamName + ", ")
                .append(inputStreamName + ".available());\n")
                .append(tab+tab+tab+tab).append("}\n")
                .append(tab+tab+tab).append("} catch (IOException e) {\n")
                .append(tab+tab+tab+tab).append("e.printStackTrace();\n")
                .append(tab+tab+tab).append("}\n")
                .append(tab+tab).append("}\n")
                .append(tab + "}\n");
    }

    private void generateBuild(Appendable target) throws IOException{
        target.append(tab+tab+tab).append("public ").append(innerClassName).append(" build(){\n")
                .append(tab+tab+tab+tab).append("query.isPrimed = false;\n")
                .append(tab+tab+tab+tab).append("return messages;\n")
                .append(tab+tab+tab).append("}\n");
    }

    private void generate3ArgConstructor(Appendable target) throws IOException{
        target.append(tab + "public ").append(interfaceClassName)
                .append("(Boolean ").append(isWritingName + ", ")
                .append("GraphManager ").append(gmName + ", ")
                .append("Pipe<RawDataSchema> ").append(sharedPipeName +"){\n");

        target.append(tab+tab).append("super(").append(gmName + ", ").append(sharedPipeName + ", ").append("NONE);\n")
                .append(tab+tab).append("this." + isWritingName).append(" = " + isWritingName + ";\n")
                .append(tab+tab).append("MessageSchemaDynamic ").append(messageSchemaName).append(" = new MessageSchemaDynamic(" + fromName + ");\n")
                .append(tab+tab).append(inPipeName).append(" = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(" + messageSchemaName + "));\n")
                .append(tab+tab).append(inPipeName).append(".initBuffers();\n")
                .append(tab+tab).append(encoderInstanceName).append(" = new ").append(encoderClassName + "(")
                .append(gmName + ", ").append(inPipeName + ", ").append(sharedPipeName + ");\n")
                .append(tab+tab).append(schedulerName).append(" = new ThreadPerStageScheduler(").append(gmName).append(");\n")
                .append(tab+tab).append(schedulerName).append(".startup();\n")
                .append(tab+tab + "isPrimed = false;\n")
                .append(tab+"}\n");
    }

    private void generate2ArgConstructor(Appendable target) throws IOException{
        target.append(tab + "public ").append(interfaceClassName)
                .append("(GraphManager ").append(gmName + ", ")
                .append("Pipe<RawDataSchema> ").append(sharedPipeName +"){\n");

        target.append(tab+tab).append("super(").append(gmName + ", ").append("NONE, ").append(sharedPipeName + ");\n")
                .append(tab+tab).append("this." + isWritingName).append(" = false;\n")
                .append(tab+tab).append("MessageSchemaDynamic ").append(messageSchemaName).append(" = new MessageSchemaDynamic(" + fromName + ");\n")
                .append(tab+tab).append(outPipeName).append(" = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(" + messageSchemaName + "));\n")
                .append(tab+tab).append(outPipeName).append(".initBuffers();\n")
                .append(tab+tab).append(decoderInstanceName).append(" = new ").append(decoderClassName + "(")
                .append(gmName + ", ").append(sharedPipeName + ", ").append(outPipeName + ");\n")
                .append(tab+tab).append(schedulerName).append(" = new ThreadPerStageScheduler(").append(gmName).append(");\n")
                .append(tab+tab).append(schedulerName).append(".startup();\n")
                .append(tab + "}\n");
    }

    private void generateAdditionalMethods(Appendable target) throws IOException{
        target.append(tab + "public Builder newBuilder(){\n")
                .append(tab+tab).append("Builder builder = new Builder();\n")
                .append(tab+tab).append("this.query = builder.query;\n")
                .append(tab+tab).append("builder.messages = this;\n")
                .append(tab+tab).append("return builder;\n")
                .append(tab + "}\n\n");

        target.append(tab + "public InventoryDetails writeTo(OutputStream ").append(outputStreamName + "){\n")
                .append(tab+tab).append("GraphManager ").append(gmName).append(" = new GraphManager();\n")
                .append(tab+tab).append("Pipe<RawDataSchema> ").append(sharedPipeName)
                .append("= new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 300));\n")
                .append(tab+tab).append(sharedPipeName).append(".initBuffers();\n")
                .append(tab+tab).append(interfaceClassName).append(" query = new ").append(interfaceClassName).append("(" + gmName)
                .append(" ," + sharedPipeName).append(");\n")
                .append(tab+tab).append("query." + outputStreamName).append(" = " + outputStreamName).append(";\n")
                .append(tab+tab).append("PipeWriter.publishWrites(").append(inPipeName + ");\n")
                .append(tab+tab).append("return this;\n")
                .append(tab + "}\n\n");
    }

    private void generateImports(Appendable target) throws IOException{
        target.append("import com.ociweb.pronghorn.pipe.*;\n")
                .append("import com.ociweb.pronghorn.pipe.build.GroceryExampleDecoderStage;\n")
                .append("import com.ociweb.pronghorn.pipe.build.GroceryExampleEncoderStage;\n")
                .append("import com.ociweb.pronghorn.stage.PronghornStage;\n")
                .append("import com.ociweb.pronghorn.stage.scheduling.GraphManager;\n")
                .append("import java.util.LinkedList;\n")
                .append("import java.io.IOException;\n")
                .append("import java.io.OutputStream;\n")
                .append("import java.io.InputStream;\n")
                .append("import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;\n\n");
    }

    private void generateLowLevelAPI(String tabspace) throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        int[] tokens = from.tokens;
        String[] scriptNames = from.fieldNameScript;
        long[] scriptIds = from.fieldIdScript;
        int i = tokens.length;
        Stack<String> stack = new Stack<String>();
        //from schema
        while (--i >= 0) {
            int type = TokenBuilder.extractType(tokens[i]);
            if (TypeMask.isLong(type)) {
                stack.push(tabspace + "Pipe.addLongValue(current." + scriptNames[i] + "a, inPipe);\n");
            } else if (TypeMask.isInt(type)) {
                stack.push(tabspace + "Pipe.addIntValue(current." + scriptNames[i] + "a, inPipe);\n");
            } else if (TypeMask.isText(type)) {
                stack.push(tabspace + "Pipe.addASCII(current." + scriptNames[i] + "a, inPipe);\n");
            }
        }
        while (!stack.empty()){
            interfaceTarget.append(stack.pop());
        }
    }

    private static String generateClassNameEncoder(MessageSchema schema) {
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

    private static String generateClassNameDecoder(MessageSchema schema) {
        if (schema instanceof MessageSchemaDynamic) {
            String name = MessageSchema.from(schema).name.replaceAll("/", "").replaceAll(".xml", "") + "DecoderStage";
            if (Character.isLowerCase(name.charAt(0))) {
                return Character.toUpperCase(name.charAt(0)) + name.substring(1);
            }
            return name;
        } else {
            return (schema.getClass().getSimpleName().replace("Schema", "")) + "Writer";
        }
    }

}
