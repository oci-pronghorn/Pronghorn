package com.ociweb.pronghorn.pipe;

public class MessageSchemaDynamic extends MessageSchema {

//    public MessageSchemaDynamic(String source) {
//        super(loadFrom(source));
//    }
//
//    private static FieldReferenceOffsetManager loadFrom(String source) {
//        try {
//            return TemplateHandler.loadFrom(source);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
    
    public MessageSchemaDynamic(FieldReferenceOffsetManager from) {
        super(from);
    }

}
