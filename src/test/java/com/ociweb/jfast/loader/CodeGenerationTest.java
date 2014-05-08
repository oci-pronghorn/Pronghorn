package com.ociweb.jfast.loader;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.generator.FASTReaderDispatchGenerator;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.stream.DispatchObserver;
import com.ociweb.jfast.stream.FASTDynamicReader;
import com.ociweb.jfast.stream.FASTReaderDispatchTemplates;
import com.ociweb.jfast.stream.FASTReaderDispatchBase;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTReaderDispatchGenExample;
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
        try {
            File classFile = new File(FASTReaderInterpreterDispatch.class.getResource(FASTReaderDispatchTemplates.class.getSimpleName() + ".class").toURI());
            String srcPath = classFile.getPath().replaceFirst("target.classes", "src/main/java").replace(".class",".java");
            File sourceFile = new File(srcPath);
            if (sourceFile.exists()) { //found source file so update resources
                String destinationString = srcPath.replaceFirst("java.com.ociweb.jfast.stream", "resources");
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
        } catch (URISyntaxException e1) {
            System.out.println("**** generation templates not copied because: "+e1.getMessage());
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
      // new SourceTemplates();
        
        FASTInput templateCatalogInput = new FASTInputByteArray(TemplateLoaderTest.buildRawCatalogData());
        TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(2048, templateCatalogInput, 32));

        // values which need to be set client side and are not in the template.
        catalog.setMessagePreambleSize((byte) 4);
        catalog.setMaxByteVectorLength(0, 0);// byte vectors are unused
        catalog.setMaxTextLength(14, 8);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));

        // this is much faster because we no longer need to jump out to OS.
        FASTInputByteArray fastInput = TemplateLoaderTest.buildInputForTestingByteArray(sourceDataFile);

        // New memory mapped solution. No need to cache because we warm up and
        // OS already has it.
        // FASTInputByteBuffer fastInput =
        // buildInputForTestingByteBuffer(sourceDataFile);

        /*
         * nate@Noah:~/github$ sudo vim /etc/sysctl.conf nate@Noah:~/github$
         * sysctl -p sysctl: permission denied on key 'vm.zone_reclaim_mode'
         * nate@Noah:~/github$ sudo sysctl -p vm.zone_reclaim_mode = 0
         * nate@Noah:~/github$
         */
        int bufferSize = 4096;// do not change without testing, 4096 is ideal.
        PrimitiveReader primitiveReader = new PrimitiveReader(bufferSize, fastInput, (2 + ((Math.max(
                catalog.maxTemplatePMapSize(), catalog.maxNonTemplatePMapSize()) + 2) * catalog.getMaxGroupDepth())));
        FASTReaderDispatchGenerator readerDispatch = 
                new FASTReaderDispatchGenerator(
                primitiveReader, catalog.dictionaryFactory(), 3, catalog.dictionaryMembers(),
                catalog.getMaxTextLength(), catalog.getMaxByteVectorLength(), catalog.getTextGap(),
                catalog.getByteVectorGap(), catalog.fullScript(), catalog.getMaxGroupDepth(), 8, 7);
        
        StringBuilder builder = new StringBuilder();
        try {
            readerDispatch.generateFullReaderSource(catalog, builder);
            
            readerDispatch.createWriteSourceClassFiles(catalog);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        System.err.println(builder.toString());
    }
    
    @Test
    public void testDecodeGenVsInterp30000() {
        // /////////
        // ensure the generated code does the same thing as the interpreted
        // code.
        // plays both together and checks each as they are processed.
        // /////////
        FASTInput templateCatalogInput = new FASTInputByteArray(TemplateLoaderTest.buildRawCatalogData());
        final TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(2048, templateCatalogInput, 32));

        // values which need to be set client side and are not in the template.
        catalog.setMessagePreambleSize((byte) 4);
        catalog.setMaxByteVectorLength(0, 0);// byte vectors are unused
        catalog.setMaxTextLength(14, 8);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();

        FASTInputByteArray fastInput1 = TemplateLoaderTest.buildInputForTestingByteArray(sourceDataFile);
        PrimitiveReader primitiveReader1 = new PrimitiveReader(2048, fastInput1, 32);
        FASTReaderInterpreterDispatch readerDispatch1 = new FASTReaderInterpreterDispatch(primitiveReader1, catalog.dictionaryFactory(),
                catalog.maxNonTemplatePMapSize(), catalog.dictionaryMembers(), catalog.getMaxTextLength(),
                catalog.getMaxByteVectorLength(), catalog.getTextGap(), catalog.getByteVectorGap(),
                catalog.fullScript(), catalog.getMaxGroupDepth(), 8, 7);
        FASTDynamicReader dynamicReader1 = new FASTDynamicReader(catalog, readerDispatch1);
        FASTRingBuffer queue1 = readerDispatch1.ringBuffer();

        FASTInputByteArray fastInput2 = TemplateLoaderTest.buildInputForTestingByteArray(sourceDataFile);
        final PrimitiveReader reader = new PrimitiveReader(2048, fastInput2, 32);
        FASTReaderDispatchBase readerDispatch2 = new FASTReaderDispatchGenExample(reader,
                catalog.dictionaryFactory(), catalog.maxNonTemplatePMapSize(), catalog.dictionaryMembers(),
                catalog.getMaxTextLength(), catalog.getMaxByteVectorLength(), catalog.getTextGap(),
                catalog.getByteVectorGap(), catalog.fullScript(), catalog.getMaxGroupDepth(), 8, 7);
        FASTDynamicReader dynamicReader2 = new FASTDynamicReader(catalog, readerDispatch2);
        FASTRingBuffer queue2 = readerDispatch2.ringBuffer();

        // final Map<Long,String> reads1 = new HashMap<Long,String>();
        // readerDispatch1.setDispatchObserver(new DispatchObserver(){
        //
        // @Override
        // public void tokenItem(long absPos, int token, int cursor, String
        // value) {
        // String msg =
        // "\n    R_"+TokenBuilder.tokenToString(token)+" id:"+(cursor>=catalog.scriptFieldIds.length?
        // "ERR": ""+catalog.scriptFieldIds[cursor])+" curs:"+cursor+
        // " tok:"+token+" "+value;
        // if (reads1.containsKey(absPos)) {
        // msg = reads1.get(absPos)+" "+msg;
        // }
        // reads1.put(absPos, msg);
        // }});

        // final Map<Long,String> reads2 = new HashMap<Long,String>();

        final int keep = 32;
        final int mask = keep - 1;
        final AtomicInteger idx = new AtomicInteger(0);
        final String[] reads2 = new String[keep];
        readerDispatch2.setDispatchObserver(new DispatchObserver() {

            @Override
            public void tokenItem(long absPos, int token, int cursor, String value) {
                String msg = " " + (PrimitiveReader.totalRead(reader) - PrimitiveReader.bytesReadyToParse(reader)) + " R_"
                        + TokenBuilder.tokenToString(token) + " id:"
                        + (cursor >= catalog.scriptFieldIds.length ? "ERR" : "" + catalog.scriptFieldIds[cursor])
                        + " curs:" + cursor + " tok:" + token + " " + value;

                reads2[mask & idx.incrementAndGet()] = msg.trim();
            }
        });

        int errCount = 0;
        int i = 0;
        while (dynamicReader1.hasMore() != 0 &&
               dynamicReader2.hasMore() != 0) {

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
                        // TODO: Z, skip pmap mismatch and look for real change
                        // all problems happen after this
                        // ASCIIOptional:001001/Default:000011/9 id:5799 curs:28
                        // tok:-1539571703

                        assertEquals(msg, int1, int2);
                    }
                }
                queue1.removeForward(1);
                queue2.removeForward(1);
                i++;
            }
        }
        assertEquals(primitiveReader1.totalRead(reader), PrimitiveReader.totalRead(reader));

    }
    
}
