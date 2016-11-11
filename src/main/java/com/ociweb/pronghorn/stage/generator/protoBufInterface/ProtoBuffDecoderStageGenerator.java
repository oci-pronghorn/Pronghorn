package com.ociweb.pronghorn.stage.generator.protoBufInterface;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
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
            target.append("private InputStream in;\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    protected void interfaceImports(Appendable target){
        try {
            target.append("import java.io.InputStream;\n");
            target.append("import java.io.IOException;\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void interfaceSetup(Appendable target){
        try {
            target.append("this.in = in;");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void appendDataSource(Appendable target){
        try {
            target.append("" +
                    "    try {\n" +
                    "        Pipe.readFieldFromInputStream(input, in, Pipe.sizeOf(input, 0)/*# of bytes to read?*/);\n" +
                    "    } catch (IOException e) {\n" +
                    "        e.printStackTrace();\n" +
                    "    }\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void additionalArgs(Appendable target){
        try {
            target.append(", InputStream in");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String generateClassName(MessageSchema schema) {
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
}
