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
    private PhastEncoderStageGenerator encoderGenerator;
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
            target.append(tab + tab + tab + "public " + varType + " get" + varNameCamel + "(){\n");
            //return variable, close off, end line.
            target.append(tab + tab + tab + tab +"return " + varNameCamel + "a;\n"
                    + tab + tab + tab + "}\n");
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
                    + ") {\n"
                    + tab + tab + tab + tab + varName + "a = " + varName + "; \n"
                    + tab + tab + tab + "} \n");
        } catch (IOException ex) {
            Logger.getLogger(ProtoBuffInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
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
                interfaceTarget.append(tab + "private long " + scriptNames[i] + "a;\n");
            } else if (TypeMask.isInt(type)) {
                interfaceTarget.append(tab + "private int " + scriptNames[i] + "a;\n");
            } else if (TypeMask.isText(type)) {
                interfaceTarget.append(tab + "private String  " + scriptNames[i] + "a;\n");
            }

        }
    }

    private void generateSetters(){
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
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
                "\n" +
                "import java.io.IOException;\n" +
                "import java.io.OutputStream;\n" +
                "\n" +
                "public class GroceryQueryProvider{\n" +
                "    private GraphManager graphManager;\n" +
                "    private Pipe<MessageSchemaDynamic> inPipe;\n" +
                "    private Pipe<RawDataSchema> sharedPipe;\n" +
                "    private Pipe<MessageSchemaDynamic> outPipe;\n" +
                "    private FieldReferenceOffsetManager from;\n" +
                "    private Boolean isWriting;\n" +
                "    private GroceryExampleEncoderStage enc;\n" +
                "    private GroceryExampleDecoderStage dec;\n" +
                "    private LinkedList<InventoryDetails> list = new LinkedList<InventoryDetails>();\n" +
                "    private OutputStream out;\n" +
                "\n" +
                "    public class InventoryDetails extends PronghornStage{\n" );
        generateInstanceVariables();
        interfaceTarget.append(
                "        private int recordsIn;\n" +
                "        private int recordsOut;\n" +
                "\n" +
                "        public InventoryDetails(){\n" +
                "            super(graphManager, NONE, outPipe);\n" +
                "        }\n" +
                "\n" +
                "        public Builder newBuilder(GraphManager gm, Boolean iswriting){\n" +
                "            graphManager = gm;\n" +
                "            isWriting = iswriting;\n" +
                "            from = GroceryExampleEncoderStage.FROM;\n" +
                "            MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(from);\n" +
                "            inPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));\n" +
                "            inPipe.initBuffers();\n" +
                "            sharedPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance));\n" +
                "            sharedPipe.initBuffers();\n" +
                "            outPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));\n" +
                "            outPipe.initBuffers();\n" +
                "            return new Builder();\n" +
                "        }\n" +
                "\n" +
                "        public void writeTo(OutputStream out){\n" +
                "            out = out;\n" +
                "            list.add(this);\n" +
                "            recordsIn++;\n" +
                "        }\n" +
                "\n" +
                "        @Override\n" +
                "        public void run(){\n" +
                "            if (isWriting){\n" +
                "                while (recordsIn > recordsOut && Pipe.hasRoomForWrite(inPipe) && !list.isEmpty()){\n" +
                "                    //write message id and stuff\n" +
                "                    InventoryDetails current = list.poll();\n" +
                "                    Pipe.addMsgIdx(inPipe, 0);\n");
        generateLowLevelAPI("                    ");
        interfaceTarget.append(
                "                    Pipe.confirmLowLevelWrite(inPipe, Pipe.sizeOf(inPipe, 0));\n" +
                "                    Pipe.publishWrites(inPipe);" +
                "\n" +
                "                    try {\n" +
                "                        Pipe.writeFieldToOutputStream(inPipe, out);\n" +
                "                    } catch (IOException e) {\n" +
                "                        e.printStackTrace();\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "            else{\n" +
                "                while (Pipe.contentRemaining(inPipe) > 0) {\n" +
                "                    //read message id and stuff\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "\n" +
                "        @Override\n" +
                "        public void startup(){\n" +
                "            if (isWriting){\n" +
                "                enc = new GroceryExampleEncoderStage(graphManager, inPipe, sharedPipe);\n" +
                "            }\n" +
                "            else{\n" +
                "                dec = new GroceryExampleDecoderStage(graphManager, sharedPipe, outPipe);\n" +
                "            }\n" +
                "        }\n" +
                "\n" +
                "\n" +
                "        public class Builder{\n" +
                "            private Builder(){\n" +
                "\n" +
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
