package com.ociweb.jfast.loader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject.Kind;
import javax.tools.ToolProvider;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.generator.FASTReaderDispatchGenerator;
import com.ociweb.jfast.generator.FASTReaderDispatchTemplates;
import com.ociweb.jfast.generator.FASTReaderSourceFileObject;
import com.ociweb.jfast.generator.FASTWriterDispatchTemplates;
import com.ociweb.jfast.generator.SourceTemplates;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.stream.DispatchObserver;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;
import com.ociweb.jfast.stream.RingBuffers;

public class CodeGenerationTest {

    /**
     * NOTE: this method is part of the build process.
     * In the test phase these source files get copied over to the resources directory.
     * At run time they are used as templates for generating code on the fly which matches the catalog.
     */
    @BeforeClass
    public static void setupTemplateResource() {
        FASTClassLoader.deleteFiles();//must always build fresh.
        System.out.println("**********************************************************************");

        copyTemplate(SourceTemplates.dispatchTemplateSourcePath(FASTReaderDispatchTemplates.class), FASTReaderDispatchTemplates.class);
        copyTemplate(SourceTemplates.dispatchTemplateSourcePath(FASTWriterDispatchTemplates.class), FASTWriterDispatchTemplates.class);
        
    }

    private static void copyTemplate(String srcPath, Class clazz) {
        File sourceFile = new File(srcPath);
        if (sourceFile.exists()) { //found source file so update resources
            String destinationString = srcPath.replaceFirst("java.com.ociweb.jfast.generator", "resources");
            File destFile = new File(destinationString);
            
            //File copy
            FileChannel source = null;
            FileChannel destination = null;
            try {

                source = new FileInputStream(sourceFile).getChannel();
                destination = new FileOutputStream(destFile).getChannel();
                destination.transferFrom(source, 0, source.size());
                System.out.println("**** generation templates copied from: "+sourceFile);
                System.out.println("****                               to: "+destFile);

            } catch (Exception e) {
                System.out.println("**** generation templates not copied because: "+e.getMessage());
            } finally {
                close(source, destination);
            }
        } else {
            System.out.println("**** generation templates not copied because source could not be found!");
        }

        //confirm that the templates are found and that runtime generation will be supported
        SourceTemplates templates = new SourceTemplates(clazz);
        if (null==templates.getRawSource()) {
            System.out.println("**** Warning, Runtime generation will not be supported because needed resources are not found.");
        } else {
            System.out.println("**** OK, Confirmed generation templates are ready for use.");
        }
        System.out.println("**********************************************************************");
    }

    private static void close(FileChannel source, FileChannel destination) {
        if (source != null) {
            try {
                source.close();
            } catch (IOException e) {
            }
        }
        if (destination != null) {
            try {
                destination.close();
            } catch (IOException e) {
            }
        }
    }
    
    @Test
    public void testCodeGenerator() {
                
        byte[] buildRawCatalogData = TemplateLoaderTest.buildRawCatalogData();
        
        FASTReaderSourceFileObject file = new FASTReaderSourceFileObject(buildRawCatalogData);
        assertEquals(Kind.SOURCE, file.getKind());
        CharSequence seq;
        try {
            seq = file.getCharContent(false);
            assertTrue(seq.length()>10); //TODO: T, may want a better test that confirms the results can be parsed.
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
    }
    
    @Test
    public void testDecodeGenVsInterp30000() {
        // /////////
        // ensure the generated code does the same thing as the interpreted
        // code.
        // plays both together and checks each as they are processed.
        // /////////
        byte[] catBytes = TemplateLoaderTest.buildRawCatalogData();
        final TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
        int maxPMapCountInBytes = TemplateCatalogConfig.maxPMapCountInBytes(catalog); 

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));

        FASTInputByteArray fastInput1 = new FASTInputByteArray(TemplateLoaderTest.buildInputArrayForTesting(sourceDataFile));
        final PrimitiveReader primitiveReader1 = new PrimitiveReader(2048, fastInput1, maxPMapCountInBytes);
        FASTReaderInterpreterDispatch readerDispatch1 = new FASTReaderInterpreterDispatch(catalog);

        
        FASTRingBuffer queue1 = RingBuffers.get(readerDispatch1.ringBuffers,0);

        FASTInputByteArray fastInput2 = new FASTInputByteArray(TemplateLoaderTest.buildInputArrayForTesting(sourceDataFile));
        final PrimitiveReader primitiveReader2 = new PrimitiveReader(2048, fastInput2, maxPMapCountInBytes);

        FASTDecoder readerDispatch2 = null;
        try {
            readerDispatch2 = DispatchLoader.loadGeneratedDispatch(catBytes, FASTClassLoader.READER);
        } catch (ReflectiveOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        } catch (SecurityException e) {
            fail(e.getMessage());
        }
        FASTRingBuffer queue2 = RingBuffers.get(readerDispatch2.ringBuffers,0);

        final int keep = 32;
        final int mask = keep - 1;
        final AtomicInteger idx = new AtomicInteger(0);


        FASTInputReactor reactor1 = new FASTInputReactor(readerDispatch1, primitiveReader1);
        FASTInputReactor reactor2 = new FASTInputReactor(readerDispatch2, primitiveReader2);
        
        
        int errCount = 0;
        int i = 0;
        while (FASTInputReactor.pump(reactor1) >= 0 && //continue if no room to read or read new message
                FASTInputReactor.pump(reactor2) >= 0) {

            while (FASTRingBuffer.contentRemaining(queue1)>0 && FASTRingBuffer.contentRemaining(queue2)>0) {
                int int1 = FASTRingBufferReader.readInt(queue1, 1);
                int int2 = FASTRingBufferReader.readInt(queue2, 1);

                if (int1 != int2) {
                    errCount++;

                    if (errCount > 1) {

                        System.err.println("back up  " + FASTRingBuffer.contentRemaining(queue1) + " fixed spots in ring buffer");

                        int c = idx.get();

                        System.err.println("From positions:"+queue1.workingTailPos+" & "+queue2.workingTailPos);
                                           
                        System.err.println("Intrp:" + Integer.toBinaryString(int1));
                        System.err.println("Compl:" + Integer.toBinaryString(int2));

                        String msg = "int " + i + " byte " + (i * 4) + "  ";
                        
                        assertEquals(msg, int1, int2);
                    }
                }
                long newValue1 = queue1.tailPos.get() + 1;
                assert (newValue1 <=queue1.workingHeadPos.value);
                queue1.removeForward2(newValue1);
                long newValue2 = queue2.tailPos.get() + 1;
                assert (newValue2 <=queue2.workingHeadPos.value);
                queue2.removeForward2(newValue2);
                i++;
            }
        }
        assertEquals(primitiveReader1.totalRead(primitiveReader1), PrimitiveReader.totalRead(primitiveReader2));

    }
    
}
