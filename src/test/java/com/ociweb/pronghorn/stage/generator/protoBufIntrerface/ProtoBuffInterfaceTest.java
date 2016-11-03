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
        
        ProtoBuffInterface test = new ProtoBuffInterface("test.package", "GroceryQueryProvider", "InventoryDetails",
                "target/generated-sources/", "src/test/resources/SIUE_GroceryStore/groceryExample.xml");
        
    }
}
