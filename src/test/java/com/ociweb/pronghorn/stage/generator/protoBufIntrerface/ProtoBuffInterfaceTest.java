/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ociweb.pronghorn.stage.generator.protoBufIntrerface;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.generator.protoBufInterface.ProtoBuffInterface;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import javax.xml.parsers.ParserConfigurationException;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 *
 * @author jake
 */
public class ProtoBuffInterfaceTest {
    @Test
    public void groceryTest() throws FileNotFoundException, ParserConfigurationException, IOException, SAXException{
        File output = new File("src/test/resources/SIUE_GroceryStore/ClassName.java");
        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/SIUE_GroceryStore/groceryExample.xml");
        MessageSchema schema = new MessageSchemaDynamic(from);
        
        PrintWriter target = new PrintWriter(output);
        
        //decode target
        File outputdecode = new File("src/test/resources/SIUE_GroceryStore/decodeTarget.java");
        PrintWriter decodeTarget = new PrintWriter(outputdecode);
        
        //encode target
        File outputencode = new File("src/test/resources/SIUE_GroceryStore/encodeTarget.java");
        PrintWriter encodeTarget = new PrintWriter(outputencode);
        
        ProtoBuffInterface test = new ProtoBuffInterface("test.package", schema, decodeTarget, encodeTarget, target, "GroceryStore");
        test.buildClass();
        target.close();
        decodeTarget.close();
        encodeTarget.close();
        
    }
}
