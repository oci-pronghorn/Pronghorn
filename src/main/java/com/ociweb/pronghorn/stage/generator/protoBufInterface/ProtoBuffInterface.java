/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ociweb.pronghorn.stage.generator.protoBufInterface;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.stage.generator.PhastDecoderStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastEncoderStageGenerator;
import java.io.IOException;

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
    
    
    public ProtoBuffInterface(String packageName, MessageSchema schema, Appendable decodeTarget, Appendable encodeTarget, Appendable interfaceTarget, String interfaceClassName){
        encoderGenerator = new PhastEncoderStageGenerator(schema, encodeTarget);
        decoderGenerator = new PhastDecoderStageGenerator(schema, decodeTarget, packageName);
        this.packageName = packageName;
        this.schema = schema;
        this.interfaceTarget = interfaceTarget;
        this.interfaceClassName = interfaceClassName;
    }
    // Do i need this if we are bringing int he encoderGenerator already?
    /*@Override
    protected void additionalTokens(Appendable target) throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        int[] tokens = from.tokens;
        String[] scriptNames = from.fieldNameScript;
        long[] scriptIds = from.fieldIdScript;
        int i = tokens.length;

        while (--i >= 0) {
            int type = TokenBuilder.extractType(tokens[i]);

            if (TypeMask.isLong(type)) {
                target.append("private long ").append(scriptNames[i]).append(";\n");
            } else if (TypeMask.isInt(type)) {
                target.append("private int ").append(scriptNames[i]).append(";\n");
            } else if (TypeMask.isText(type)) {
                target.append("private String ").append(scriptNames[i]).append(";\n");
            }
        }
    }
    */
    //public String getName() { return name; }
    protected void InterfaceBuilderGetterString(MessageSchema schema, Appendable interfaceTarget, String stringName) {
        try {
            appendStaticCall(interfaceTarget, encoderGenerator, "InterfaceBuilderGetter")
                    .append("public String get")
                    .append(stringName.toUpperCase()).append("() { return ")
                    .append(stringName).append("; }")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //public void setName(String name) { this.name = name; }
    protected void InterfaceBuilderSetterString(MessageSchema schema, Appendable interfaceTarget, String stringName) {
        try {
            appendStaticCall(interfaceTarget, encoderGenerator, "InterfaceBuilderSetter")
                    .append("public void set")
                    .append(stringName.toUpperCase()).append("(String ")
                    .append(stringName).append(") { this.")
                    .append(stringName).append(" = ")
                    .append(stringName).append("; }")
                    .append(");\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public class interfaceClassName {
        
    }
    
}
