package com.ociweb.jfast.catalog;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.jfast.catalog.generator.CatalogGenerator;
import com.ociweb.jfast.catalog.generator.TemplateGenerator;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateHandler;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;

public class CatalogGeneratorTest {
    
    int[] numericTypes = new int[] {
            TypeMask.IntegerUnsigned,
            TypeMask.IntegerUnsignedOptional,
            TypeMask.IntegerSigned,
            TypeMask.IntegerSignedOptional,
            TypeMask.LongUnsigned,
            TypeMask.LongUnsignedOptional,
            TypeMask.LongSigned,
            TypeMask.LongSignedOptional,
            TypeMask.Decimal,
            TypeMask.DecimalOptional
    };
    
    int[] numericOps = new int[] {
            OperatorMask.Field_Constant,
            OperatorMask.Field_Copy,
            OperatorMask.Field_Default,
            OperatorMask.Field_Delta,
            OperatorMask.Field_Increment,            
            OperatorMask.Field_None 
    };
    
    @Test
    public void numericFieldTest() {
                        
        String name = "testTemplate";
        int id = 2;
        boolean reset = false;
        String dictionary = null;
        boolean fieldPresence = false;
     
        String fieldInitial = "10";
        
        int totalFields = 10;
                        
        
        int fieldId = 1000;
        
        int p = numericOps.length;
        while (--p>=0) {
            
            int fieldOperator = numericOps[p];
            
            int t = numericTypes.length;
            while (--t>=0) {        
                
                int fieldType = numericTypes[t];
                
             //   System.err.println();
           //     System.err.println("checking:"+TypeMask.xmlTypeName[fieldType]+" "+OperatorMask.xmlOperatorName[fieldOperator]);
                
                CatalogGenerator cg = new CatalogGenerator();
                TemplateGenerator template = cg.addTemplate(name, id, reset, dictionary);            
                
                
                int f = totalFields; //TODO: do each one to capture the linear scalablility
                while (--f>=0) {
                    String fieldName = "field"+fieldId;
                    template.addField(fieldName, fieldId++, fieldPresence, fieldType, fieldOperator, fieldInitial);        
                }
                
                StringBuilder builder = cg.appendTo("", new StringBuilder());        
    
       //         System.err.println(builder);
                
                ClientConfig clientConfig = new ClientConfig();                
                byte[] catBytes = convertTemplateToCatBytes(builder, clientConfig);        
                TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
                
                assertEquals(1, catalog.templatesCount());
                
                //TODO: A, new unit tests. use catalog to test mock data
                FASTClassLoader.deleteFiles();
                FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes); 
                
                
                int writeBuffer=1024;
                byte[] buffer = new byte[1024];
                FASTOutput fastOutput = new FASTOutputByteArray(buffer );
                int maxGroupCount=1024;
                //TODO: A, need the maximum groups that would fit in this buffer based on smallest known buffer.
                //catalog.getMaxGroupDepth()
                
                PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput, maxGroupCount, true);
                
                
                //FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, queue, writerDispatch);
                
         //       System.err.println(Arrays.toString(catalog.getScriptTokens()));                        
                
            }            
        } 
        
        
        
    }



    public byte[] convertTemplateToCatBytes(StringBuilder builder, ClientConfig clientConfig) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gZipOutputStream = new GZIPOutputStream(baos);
            FASTOutput output = new FASTOutputStream(gZipOutputStream);
            
            SAXParserFactory spfac = SAXParserFactory.newInstance();
            SAXParser sp = spfac.newSAXParser();
            InputStream stream = new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8));           
            
            TemplateHandler handler = new TemplateHandler(output, clientConfig);            
            sp.parse(stream, handler);
    
            handler.postProcessing();
            gZipOutputStream.close();            
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        byte[] catBytes = baos.toByteArray();
        return catBytes;
    }
    
    
    
    public static byte[] buildRawCatalogData(ClientConfig clientConfig) {
        //this example uses the preamble feature
        clientConfig.setPreableBytes((short)4);

        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, "/performance/example.xml", clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue("Catalog must be built.", catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }
    
    
    

}
