package com.ociweb.pronghorn.stage.generator;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelWriter;

public class PhastDecoderStageGenerator extends TemplateProcessGeneratorLowLevelWriter{

    public PhastDecoderStageGenerator(MessageSchema schema, Appendable target, String packageName) {
        super(schema, target, /*isAbstract*/ false, packageName);
        // TODO Auto-generated constructor stub
    }
    

    @Override
    protected void additionalImports(MessageSchema schema, Appendable target) {
        try {
            target.append("import ").append(schema.getClass().getCanonicalName()).append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    
    //TODO:...

}
