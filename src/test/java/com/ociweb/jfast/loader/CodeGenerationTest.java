package com.ociweb.jfast.loader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.tools.JavaFileObject.Kind;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.generator.FASTReaderDispatchGenerator;
import com.ociweb.jfast.generator.FASTReaderDispatchTemplates;
import com.ociweb.jfast.generator.FASTWriterDispatchTemplates;
import com.ociweb.jfast.generator.SimpleSourceFileObject;
import com.ociweb.jfast.generator.SourceTemplates;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.RingReader;

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
        FASTClassLoader.deleteFiles();
        
        byte[] buildRawCatalogData = TemplateLoaderTest.buildRawCatalogData(new ClientConfig());
        
        SimpleSourceFileObject file = 
        		new SimpleSourceFileObject(FASTClassLoader.SIMPLE_READER_NAME,
        				new FASTReaderDispatchGenerator(buildRawCatalogData, new ArrayList(), TemplateCatalogConfig.buildRingBuffers(new TemplateCatalogConfig(buildRawCatalogData), (byte)8, (byte)18)).generateFullSource(new StringBuilder()));

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
        int rbPrimaryRingBits = 9;
        int rbTextRingBits = 16;
        
        FASTClassLoader.deleteFiles();
        // /////////
        // ensure the generated code does the same thing as the interpreted
        // code.
        // plays both together and checks each as they are processed.
        // /////////
        byte[] catBytes = TemplateLoaderTest.buildRawCatalogData(new ClientConfig());
        final TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
        int maxPMapCountInBytes = TemplateCatalogConfig.maxPMapCountInBytes(catalog); 

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));

        FASTInputByteArray fastInput1 = new FASTInputByteArray(TemplateLoaderTest.buildInputArrayForTesting(sourceDataFile));
        final PrimitiveReader primitiveReader1 = new PrimitiveReader(2048, fastInput1, maxPMapCountInBytes);
        FASTReaderInterpreterDispatch readerDispatch1 = new FASTReaderInterpreterDispatch(catalog, RingBuffers.buildNoFanRingBuffers(new RingBuffer(new RingBufferConfig((byte)rbPrimaryRingBits, (byte)rbTextRingBits, catalog.ringByteConstants(), catalog.getFROM()))));

        
        RingBuffer queue1 = RingBuffers.get(readerDispatch1.ringBuffers,0);

        FASTInputByteArray fastInput2 = new FASTInputByteArray(TemplateLoaderTest.buildInputArrayForTesting(sourceDataFile));
        final PrimitiveReader primitiveReader2 = new PrimitiveReader(2048, fastInput2, maxPMapCountInBytes);

        FASTDecoder readerDispatch2 = null;
        try {
            readerDispatch2 = DispatchLoader.loadGeneratedReaderDispatch(catBytes, FASTClassLoader.READER, RingBuffers.buildNoFanRingBuffers(new RingBuffer(new RingBufferConfig((byte)rbPrimaryRingBits, (byte)rbTextRingBits, catalog.ringByteConstants(), catalog.getFROM()))));
        } catch (ReflectiveOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        } catch (SecurityException e) {
            fail(e.getMessage());
        }
        RingBuffer queue2 = RingBuffers.get(readerDispatch2.ringBuffers,0);


        FASTReaderReactor reactor1 = new FASTReaderReactor(readerDispatch1, primitiveReader1);
        FASTReaderReactor reactor2 = new FASTReaderReactor(readerDispatch2, primitiveReader2);
        
        
        int errCount = 0;
        int i = 0;
        while (FASTReaderReactor.pump(reactor1) >= 0 && //continue if no room to read or read new message
               FASTReaderReactor.pump(reactor2) >= 0) {

            while (RingBuffer.contentRemaining(queue1)>0 && RingBuffer.contentRemaining(queue2)>0) {
				int int1 = RingBuffer.readInt(queue1.buffer, queue1.mask, ++queue1.workingTailPos.value);
                int int2 = RingBuffer.readInt(queue2.buffer, queue2.mask, ++queue2.workingTailPos.value);

                //System.err.println(i+" "+int1+"  "+int2);
                
                if (int1 != int2) {
                    errCount++;

                    if (errCount > 1) {

                        System.err.println("back up  " + RingBuffer.contentRemaining(queue1) + " fixed spots in ring buffer. From positions:"+queue1.workingTailPos.value+" & "+queue2.workingTailPos.value);
                                           
                        System.err.println("Value from Intrp:" + Integer.toBinaryString(int1));
                        System.err.println("Value from Compl:" + Integer.toBinaryString(int2));

                        String msg = "int " + i + " byte " + (i * 4) + "  ";
                        
                        assertEquals(msg, int1, int2);
                    }
                }
                
                RingBuffer.releaseReadLock(queue1);
                RingBuffer.releaseReadLock(queue2);
                
                
				i++;
            }
        }
        assertEquals(PrimitiveReader.totalRead(primitiveReader1), PrimitiveReader.totalRead(primitiveReader2));

    }
    
}
