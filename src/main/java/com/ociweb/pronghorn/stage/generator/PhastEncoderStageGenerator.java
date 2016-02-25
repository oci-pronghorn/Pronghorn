package com.ociweb.pronghorn.stage.generator;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelReader;

public class PhastEncoderStageGenerator extends TemplateProcessGeneratorLowLevelReader{

    public PhastEncoderStageGenerator(MessageSchema schema, Appendable bodyTarget) {
        super(schema, bodyTarget);
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
    
    
    
    
    //TODO: ....

}
