package com.ociweb.pronghorn.stage.generator.protoBufInterface;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.stage.generator.PhastEncoderStageGenerator;

import java.io.IOException;

/**
 * Created by jake on 11/7/16.
 */
public class ProtoBuffEncoderStageGenerator extends PhastEncoderStageGenerator {

    public ProtoBuffEncoderStageGenerator(MessageSchema schema, Appendable bodyTarget){
        super(schema, bodyTarget);
    }

    protected void interfaceMembers(Appendable target){
        try {
            target.append("private OutputStream out;\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void additionalBody(Appendable target){
        try {
            target.append("   try {\n" +
                    "      Pipe.writeFieldToOutputStream(output, out);\n" +
                    "   } catch (IOException e) {\n" +
                    "      e.printStackTrace();\n" +
                    "   }\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void additionalConstructorLogic(Appendable target){
        try {
            target.append("this.out = out;\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void additionalArgs(Appendable target){
        try {
            target.append(", OutputStream out");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void interfaceImports(Appendable target){
        try {
            target.append("import java.io.OutputStream;\n");
            target.append("import java.io.IOException;\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
