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


}
