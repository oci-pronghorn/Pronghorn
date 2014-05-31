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
import com.ociweb.jfast.generator.FASTReaderDispatchGenerator;
import com.ociweb.jfast.generator.FASTReaderSourceFileObject;
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

public class CodeGenerationTest {

    /**
     * NOTE: this method is part of the build process.
     * In the test phase these source files get copied over to the resources directory.
     * At run time they are used as templates for generating code on the fly which matches the catalog.
     */
    @BeforeClass
    public static void setupTemplateResource() {
        System.out.println("**********************************************************************");

        String srcPath = SourceTemplates.readerDispatchTemplateSourcePath();
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
        SourceTemplates templates = new SourceTemplates();
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
        final TemplateCatalog catalog = new TemplateCatalog(catBytes);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));

        FASTInputByteArray fastInput1 = TemplateLoaderTest.buildInputForTestingByteArray(sourceDataFile);
        final PrimitiveReader primitiveReader1 = new PrimitiveReader(2048, fastInput1, 32);
        FASTReaderInterpreterDispatch readerDispatch1 = new FASTReaderInterpreterDispatch(catalog);

        
        FASTRingBuffer queue1 = readerDispatch1.ringBuffer(0);

        FASTInputByteArray fastInput2 = TemplateLoaderTest.buildInputForTestingByteArray(sourceDataFile);
        final PrimitiveReader primitiveReader2 = new PrimitiveReader(2048, fastInput2, 33);

        FASTDecoder readerDispatch2 = null;
        try {
            readerDispatch2 = DispatchLoader.loadGeneratedDispatchReader(catBytes);
        } catch (ReflectiveOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        } catch (SecurityException e) {
            fail(e.getMessage());
        }
        FASTRingBuffer queue2 = readerDispatch2.ringBuffer(0);

        final int keep = 32;
        final int mask = keep - 1;
        final AtomicInteger idx = new AtomicInteger(0);
        final String[] reads2 = new String[keep];
        readerDispatch2.setDispatchObserver(new DispatchObserver() {

            @Override
            public void tokenItem(long absPos, int token, int cursor, String value) {
                String msg = " " + (PrimitiveReader.totalRead(primitiveReader1) - PrimitiveReader.bytesReadyToParse(primitiveReader2)) + " R_"
                        + TokenBuilder.tokenToString(token) + " id:"
                        + (cursor >= catalog.scriptFieldIds.length ? "ERR" : "" + catalog.scriptFieldIds[cursor])
                        + " curs:" + cursor + " tok:" + token + " " + value;

                reads2[mask & idx.incrementAndGet()] = msg.trim();
            }
        });

        int errCount = 0;
        int i = 0;
        while (FASTInputReactor.select(readerDispatch1, primitiveReader1, queue1) != 0 &&
                FASTInputReactor.select(readerDispatch2, primitiveReader2, queue2) != 0) {

            while (queue1.hasContent() && queue2.hasContent()) {
                int int1 = FASTRingBufferReader.readInt(queue1, 1);
                int int2 = FASTRingBufferReader.readInt(queue2, 1);

                if (int1 != int2) {
                    errCount++;

                    if (errCount > 1) {

                        System.err.println("back up  " + queue1.contentRemaining() + " fixed spots in ring buffer");

                        int c = idx.get();
                        int j = keep;
                        while (--j >= 0) {
                            System.err.println(j + " " + reads2[mask & (c - j)]);
                        }
                        System.err.println("1:" + Integer.toBinaryString(int1));
                        System.err.println("2:" + Integer.toBinaryString(int2));

                        String msg = "int " + i + " byte " + (i * 4) + "  ";
                        // TODO: Z, regenerate code for this section that does
                        // not match.

                        assertEquals(msg, int1, int2);
                    }
                }
                queue1.removeForward(1);
                queue2.removeForward(1);
                i++;
            }
        }
        assertEquals(primitiveReader1.totalRead(primitiveReader1), PrimitiveReader.totalRead(primitiveReader2));

    }
    
}
