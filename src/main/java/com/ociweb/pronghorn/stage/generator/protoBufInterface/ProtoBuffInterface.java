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
    private PhastDecoderStageGenerator decoderGenerator;
    private ProtoBuffEncoderStageGenerator encoderGenerator;
    private String interfaceClassName;
    private String packageName;
    String innerClassName;
    private MessageSchema schema;
    private Appendable interfaceTarget;
    private static String tab = "    ";
    private String decoderClassName;
    private String encoderClassName;
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
            target.append(tab + tab + tab + "public " + varType + " get" + varNameCamel + "(){\n");
            //return variable, close off, end line.
            if (varType == "int")
                target.append( tab + tab + tab + tab + "return PipeReader.readInt(query.inPipe, query." + varName + "loc); \n");
            if (varType == "long")
                target.append( tab + tab + tab + tab + "return PipeReader.readLong(query.inPipe, query." + varName + "loc); \n");
            if (varType == "String") {
                target.append(tab + tab + tab + tab + "StringBuilder str = new StringBuilder();\n");
                target.append(tab + tab + tab + tab + "PipeReader.readASCII(query.inPipe, query." + varName + "loc, str); \n");
                target.append(tab + tab + tab + tab + "return str.toString(); \n");
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

    private void additionalInstaceVariables(){
        try {
            interfaceTarget.append(
                    "        private GraphManager gm;\n" +
                    "        private Pipe<MessageSchemaDynamic> inPipe;\n" +
                    "        private Pipe<MessageSchemaDynamic> outPipe;\n" +
                    "        private Pipe<RawDataSchema> trandsmittedPipe;\n" +
                    "        private GroceryExampleEncoderStage enc;\n" +
                    "        GroceryExampleDecoderStage dec;\n" +
                    "        OutputStream out;\n" +
                    "        ThreadPerStageScheduler scheduler;\n");
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
        interfaceTarget.append("THIS CLASS HAS BEN GENERATED DO NOT MODIFY\n");
        interfaceTarget.append("*/\n");
        interfaceTarget.append("package "+ packageName + ";\n" +
                "\n" +
                "import com.ociweb.pronghorn.pipe.*;\n" +
                "import com.ociweb.pronghorn.pipe.build.GroceryExampleDecoderStage;\n" +
                "import com.ociweb.pronghorn.pipe.build.GroceryExampleEncoderStage;\n" +
                "import com.ociweb.pronghorn.stage.PronghornStage;\n" +
                "import com.ociweb.pronghorn.stage.scheduling.GraphManager;\n" +
                "import java.util.LinkedList;\n" +
                "import java.io.IOException;\n" +
                "import java.io.OutputStream;\n" +
                "import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;\n\n" +
                "public class GroceryQueryProvider{\n");
        from.appendConstuctionSource(interfaceTarget);
        generateLOC("InventoryDetails");
        additionalInstaceVariables();
        generateConstructor();
        interfaceTarget.append(
                "    public class InventoryDetails{\n" );
        interfaceTarget.append(
                "\n" +
                "        private GroceryQueryProvider query;\n" +
                "        public Builder new Builder(){\n" +
                "            Builder builder = newBuilder();\n" +
                "            this.query = builder.query;\n" +
                "            PipeWriter.tryWriteFragment(inPipe, 0);\n" +
                "            return builder;" +
                "        }\n" +
                "\n" +
                "        public void writeTo(OutputStream out){\n" +
                "            query.out = out;" +
                "            PipeWriter.publishWrites(inPipe);" +
                "        }\n" +
                "        public class Builder{\n" +
                "            private GroceryQueryProvider query;\n" +
                "            private Builder(){\n" +
                "            query = new GroceryQueryProvider(true);\n" +
                "            }\n" +
                "            //setters\n");
        generateSetters();
        generateGetters();
        interfaceTarget.append(
                "            public InventoryDetails build(){\n" +
                "                return new InventoryDetails();\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}");


    }

    private void generateConstructor(){
        try {
            interfaceTarget.append("" +
                    "   public GroceryQueryProvider(Boolean isWriting){\n" +
                    "        gm = new GraphManager();\n" +
                    "        MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(FROM);\n" +
                    "        trandsmittedPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance));\n" +
                    "        trandsmittedPipe.initBuffers();\n" +
                    "        if(isWriting) {\n" +
                    "            inPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));\n" +
                    "            inPipe.initBuffers();\n" +
                    "            enc = new GroceryExampleEncoderStage(gm, inPipe, trandsmittedPipe, out);\n" +
                    "        }else{\n" +
                    "            outPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));\n" +
                    "            outPipe.initBuffers();\n" +
                    "            dec = new GroceryExampleDecoderStage(gm, trandsmittedPipe, outPipe);\n" +
                    "        }\n" +
                    "        scheduler = new ThreadPerStageScheduler(gm);\n" +
                    "        scheduler.startup();\n" +
                    "    }\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
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
