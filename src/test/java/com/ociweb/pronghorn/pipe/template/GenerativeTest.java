package com.ociweb.pronghorn.pipe.template;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.schema.generator.CatalogGenerator;
import com.ociweb.pronghorn.pipe.schema.generator.TemplateGenerator;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.pipe.stream.StreamingWriteVisitorGenerator;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;

public class GenerativeTest {
    
    //TODO: A, THOUGHT use "events" not "messages" in notation, this will lead to event source designs

    
    //TODO:M, investigate the mapping of reactive streams interface on to the pipes.
    
    
    
    int messages = 100;
    int varLength = 64;

    
   @Test
   public void mostImporantCoverageTest() {
       
       StringBuilder schema = new StringBuilder();
       
       generateCoveringTestSchema(schema);
           
       FieldReferenceOffsetManager from = loadFrom(schema.toString());
       PipeConfig rbConfig = new PipeConfig(new MessageSchemaDynamic(from), messages, varLength);
       
       int commonSeed = 300;   
       int iterations = 2;
       
       Pipe ring1 = buildPopulatedRing(from, rbConfig, commonSeed, iterations);
       Pipe ring2 = buildPopulatedRing(from, rbConfig, commonSeed, iterations);
       
       //confirm that both rings contain the exact same thing
       assertTrue(Arrays.equals(Pipe.primaryBuffer(ring1), Pipe.primaryBuffer(ring2)));
       assertTrue(Arrays.equals(Pipe.byteBuffer(ring1), Pipe.byteBuffer(ring2)));
       
       //////////////////////////
       //now starts the real test, we need to read/write these values, and check them against the original 
       /////////////////////////
       
       
       //while try read from token look up the right read type.
       //reads all the messsages until the ring is empty

       Pipe ring1byReadWrite = new Pipe(rbConfig);
       Pipe ring1byCopy = new Pipe(rbConfig);
       
       
       ring1byReadWrite.initBuffers();
       
       int messageIdx = 0;//first and only messsage;
       while (PipeReader.tryReadFragment(ring1) && PipeWriter.tryWriteFragment(ring1byReadWrite, messageIdx)) {
           int msgId = PipeReader.getMsgIdx(ring1);
           
           int scriptSize = from.fragScriptSize[msgId];
           
           int s = 0;
           int fieldId = 1000;
           
           while (++s<scriptSize) {
               int idx = msgId+s;
               fieldId++;
               long fromFieldId = from.fieldIdScript[idx];
               if (0!= fromFieldId && fromFieldId!=fieldId) {
                   fail("Did not find expected field id "+fieldId+" != "+fromFieldId);
               }
               
               //this is a slow linear search repeated for each message, TODO: B, replace this with an array of pre-build values
               int fieldLOC = 0==fromFieldId?0:FieldReferenceOffsetManager.lookupFieldLocator(fromFieldId, msgId, from);
               
               int token = from.tokens[idx];               
               int type = TokenBuilder.extractType(token);
               
               switch (type) {
                   case TypeMask.IntegerUnsigned:
                   case TypeMask.IntegerUnsignedOptional:
                   case TypeMask.IntegerSigned:
                   case TypeMask.IntegerSignedOptional:
                       int intValue = PipeReader.readInt(ring1, fieldLOC);                       
                       float floatValue = PipeReader.readIntBitsToFloat(ring1, fieldLOC);
                       int expectedInt = Float.floatToRawIntBits(floatValue);
                       if (intValue!=expectedInt) {
                           fail();
                       }
                       PipeWriter.writeInt(ring1byReadWrite, fieldLOC, intValue);
                       
                       break;
                   case TypeMask.LongUnsigned:
                   case TypeMask.LongUnsignedOptional:
                   case TypeMask.LongSigned:
                   case TypeMask.LongSignedOptional:
                       long longValue = PipeReader.readLong(ring1, fieldLOC);
                       double doubleValue = PipeReader.readLongBitsToDouble(ring1, fieldLOC);
                       long expectedLong = Double.doubleToRawLongBits(doubleValue);
                       if (longValue!=expectedLong) {
                           fail();
                       }
                       
                       break;
                   case TypeMask.TextASCII:
                   case TypeMask.TextASCIIOptional:
                       PipeReader.readASCII(ring1, fieldLOC, new StringBuilder());
                       break;
                   case TypeMask.TextUTF8:
                   case TypeMask.TextUTF8Optional:
        //               RingReader.readUTF8(ring1, fieldLOC, new StringBuilder());
                       break;
                   case TypeMask.Decimal:
                   case TypeMask.DecimalOptional:
                       PipeReader.readDecimalExponent(ring1, fieldLOC);
                       PipeReader.readDecimalMantissa(ring1, fieldLOC);                       
                       break;
                   case TypeMask.ByteVector:
                   case TypeMask.ByteVectorOptional:
            //           RingReader.readBytes(ring1, fieldLOC, ByteBuffer.allocate(70));
                       break;
               }
               
               
               if (TypeMask.Decimal==type || TypeMask.DecimalOptional==type) {
                   s++;//extra slot for the long
               }
                 
           }
       }
       
       PipeReader.releaseReadLock(ring1); //once per message.
       
       
       
       
       
       
       
       
       

System.err.println(ring1);
       
       
       
       
       //load schema and generate test data
       
       
       
       //generate test data off generated template files
       //test read and write of all data
         //use visitor API
         //use high level RingReader/RingWalker
       
   }

public Pipe buildPopulatedRing(FieldReferenceOffsetManager from,
        PipeConfig rbConfig, int commonSeed, int iterations) {
    int i;
     	Pipe ring2 = new Pipe(rbConfig);
       ring2.initBuffers();
       StreamingWriteVisitorGenerator swvg2 = new StreamingWriteVisitorGenerator(from, new Random(commonSeed), varLength, varLength);    
       StreamingVisitorWriter svw2 = new StreamingVisitorWriter(ring2, swvg2);
       svw2.startup();     
       i = iterations;
       while (--i>0) {
           svw2.run();
       }
    return ring2;
}

    public FieldReferenceOffsetManager loadFrom(String schema) {
           InputStream inputStream = new ByteArrayInputStream(schema.getBytes());
           try {
               return TemplateHandler.loadFrom(inputStream);
           } catch (Throwable e) {
               throw new RuntimeException(e);
           }
    }

private void generateCoveringTestSchema(StringBuilder schema) {
    boolean templateReset = false;
       String templateDictionary = "none";
       
       
       
       String templateName = "generatedTemplate";
       int templateId = 1001;
       
       CatalogGenerator generator = new CatalogGenerator();
       TemplateGenerator template = generator.addTemplate(templateName, templateId, templateReset, templateDictionary);
       
       String[] initialValues = new String[] {null, "", "1", "2", "1000000000", "-1", "-2"};
       int[] extraOpp = new int[] {OperatorMask.Field_Increment,OperatorMask.Field_Increment,OperatorMask.Field_Increment,OperatorMask.Field_Increment,
                                   OperatorMask.Field_Increment,OperatorMask.Field_Increment,OperatorMask.Field_Increment,OperatorMask.Field_Increment,
                                   OperatorMask.Field_Tail,OperatorMask.Field_Tail,OperatorMask.Field_Tail,OperatorMask.Field_Tail,
                                   OperatorMask.Field_Increment,OperatorMask.Field_Increment,OperatorMask.Field_Tail,OperatorMask.Field_Tail};                                
              
       int id=1000;
       
       
       int t = TypeMask.Group; //all the values less than this are simple
       while (--t>=0) {
           int ops = 5; //0-4 are common to all types
           while (--ops>=0) {
               int init = initialValues.length;
               while (--init>=0) {
                   id++;
                   template.addField("field"+id, id, 0!=(1&t), t, ops, initialValues[init]);               
               }
           }
           ops = extraOpp[t];
           int init = initialValues.length;
           while (--init>=0) {
               id++;
               template.addField("field"+id, id, 0!=(1&t), t, ops, initialValues[init]);               
           }         
           
       }
       
       try {
           generator.appendTo("", schema);
       } catch (IOException e) {
           throw new RuntimeException(e);
       }
}
    
    
}
