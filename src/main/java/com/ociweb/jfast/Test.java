package com.ociweb.jfast;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.loader.ClientConfig;
import com.ociweb.jfast.loader.FieldReferenceOffsetManager;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.loader.TemplateLoader;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;

public class Test {

    public static void main(String[] args) {
        
        //this example uses the preamble feature
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setPreableBytes((short)4);
        String templateSource = "/performance/example.xml";
        String dataSource = "/performance/complex30000.dat";
        
        new Test().decode(clientConfig, templateSource, dataSource);
    }
    
    public void decode(ClientConfig clientConfig, String templateSource, String dataSource) {
         int count = 1024000;
        
                
          byte[] catBytes = buildRawCatalogData(clientConfig, templateSource);
          TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes); 

          // connect to file

          //TODO: AA, API design flaw, most will not want to compute this mess.
          int maxPMapCountInBytes = 2 + ((Math.max(
                  catalog.maxTemplatePMapSize(), catalog.maxNonTemplatePMapSize()) + 2) * catalog.getMaxGroupDepth());             
          
          
          FASTClassLoader.deleteFiles();
          FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes);
          

          int msgs = 0;
          int queuedBytes = 0;

          int iter = count;
          while (--iter >= 0) {
              FASTInputStream fastInputStream = new FASTInputStream(testDataInputStream(dataSource));
              PrimitiveReader reader = new PrimitiveReader(1024*1024*2,fastInputStream, maxPMapCountInBytes);
              
              PrimitiveReader.fetch(reader);//Pre-load the file so we only count the parse time.
              FASTInputReactor reactor = new FASTInputReactor(readerDispatch, reader);
              
              FASTRingBuffer queue = readerDispatch.ringBuffer(0);
              System.gc();
              
              msgs = 0;
              
              //TODO: A, API bug, must be able to set the ring buffer size and fragment size check must establish minimum.
                            
              //TODO: AA, API incomplete
              
              //should always look up the field constants once before usages but this also must be redone if template changes.
              //int myField = catalog.lookupField("templateName","fieldName"); Only do this against known template
              
              
              FieldReferenceOffsetManager from = catalog.getFROM();
              
              //these two only happen at the beginning of a new template.
              int preambleOffset = from.preambleOffset; 
              int templateOffset = from.templateOffset;
              
              
           //   int fragmentSize = from.lookupFragmentSize(fragmentId);
              
              
              double start = System.nanoTime();
              int templateId = -1;
              int flag;
              while (0 != (flag = reactor.select())) {
                  //In this single threaded example we will consume every fragment as it
                  //is written to the ring buffer, as a result the ring buffer will always
                  //return to empty after every cycle and should not block further reads unless
                  //it has been initialized smaller than one of the fragments.
                  
                  if (flag<0) { //May notify internal error log that data could not be moved for this reason
                      //TODO: API A, select failure reasons, NoIncomingData, NoRoomForOutgoingData, NoError,  -1, 0 , 1
                      //out of space in ring buffer so we must consume some messages
                      if (!queue.hasContent()) {
                          System.err.println("The internal buffer is not large enough to hold the fragment.");
                          System.exit(-1);
                      } else {
                          System.err.println("odd, this should not have happened");
                          //because every fragment is read as it is is written this is not expected to be called.
                          FASTRingBuffer.dump(queue);
                      }                     
                  } else {
                      
                      //we have a new fragment to read
                      if (templateId<0) { //TODO: A, API perhaps flag should return beginning of template?
                          templateId = FASTRingBufferReader.readInt(queue, templateOffset);
                          
                          int cursor = catalog.getTemplateStartIdx()[templateId];
                          //this cursor will end with 
                          //  - fixed stop
                          //  - dyn jump between 2 locations based on count in stack. (may come from pmap bit if group is optional)
                          //this cursor has a fixed size with it.
                          
                          //TODO: AA, need test for optional groups this is probably broken. 
                          //we know that each of these templates can only go to a specific fragment next.
                          //this is determined by the length if there is one or it is fixed.
                          
                          
                      }
                      
                      //must read fragment id!
                      //this is the position in the script
                      //if select returns the script location we can use that but what about threaded cases?
                      
                      //reader can walk a script on client side to know the state of the next fragment without adding it to ring buffer.
                      
                      //stack in ring buffer allows reading of fields still in ring
                      
                      //need client stack of nestedd seq etc. to know when we switch to the next one.
                      //at end of fragment length# will tell repeat for next sequence.
                      
                      
                      /*
                      //TODO: API, AA need consistant way to know what kind of frament we have without data from select!!
                      
                      ## if the fragment id is always on the front of the fragment in ring buffer it will 
                         Be limited by the memory write speeds and more data will move
                         Be easy to determine the fragment in the client
                         
                      ## if the id is passed back from select it will
                         Make it difficult to code multithreaded setup.
                         Be easy for single threaded app to read the fragment
                         
                      ## if the client side follows along with a helper script class
                         A dedicated walker class will need to be written
                         The decoder will not need to do more work.
                         Every thread will be able to run at its own pace.
                      
                      TODO: AAA, URGENT change, Knowing the end of a template is discovered by having walked an by no other means.
                      
                      */
                      
                      queuedBytes += 1;
                      FASTRingBufferReader.dump(queue);
                      //
                      
                      
                      //this fragment marks the end of a message
                      if (0 != (flag & TemplateCatalogConfig.END_OF_MESSAGE)) {
                          msgs++;
                          templateId = -1;
                      }
                      
                  }
                  
              }

              double duration = System.nanoTime() - start;
              
              if (shouldPrint(iter)) {
                  printSummary(msgs, queuedBytes, duration, fastInputStream.totalBytes()); 
              }

              // //////
              // reset the dictionary to run the test again.
              // //////
              readerDispatch.reset(catalog.dictionaryFactory());

          }

      }

    private boolean shouldPrint(int iter) {
        return (0x7F & iter) == 0;
    }

    private void printSummary(int msgs, int queuedBytes, double duration, long totalTestBytes) {
        int ns = (int) duration;
          float mmsgPerSec = (msgs * (float) 1000l / ns);
          float nsPerByte = (ns / (float) totalTestBytes);
          int mbps = (int) ((1000l * totalTestBytes * 8l) / ns);

          System.err.println("Duration:" + ns + "ns " + " " + mmsgPerSec + "MM/s " + " " + nsPerByte + "nspB "
                  + " " + mbps + "mbps " + " In:" + totalTestBytes + " Out:" + queuedBytes + " pct "
                  + (totalTestBytes / (float) queuedBytes) + " Messages:" + msgs);
    }
    
    //TODO: A, Where does the lookup go?
    public static int stepSizeInRingBuffer(int token) {
        //TODO: C, Convert to array lookup
        
        int stepSize = 0;
        if (0 == (token & (16 << TokenBuilder.SHIFT_TYPE))) {
            // 0????
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 00???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // int
                    stepSize = 1;
                } else {
                    // long
                    stepSize = 2;
                }
            } else {
                // 01???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // int for text (takes up 2 slots)
                    stepSize = 2;
                } else {
                    // 011??
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // 0110? Decimal and DecimalOptional
                        stepSize = 3;
                    } else {
                        // int for bytes
                        stepSize = 2;
                    }
                }
            }
        } else {
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 10???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // 100??
                    // Group Type, no others defined so no need to keep checking
                    stepSize = 0;
                } else {
                    // 101??
                    // Length Type, no others defined so no need to keep
                    // checking
                    // Only happens once before a node sequence so push it on
                    // the count stack
                    stepSize = 1;
                }
            } else {
                // 11???
                // Dictionary Type, no others defined so no need to keep
                // checking
                stepSize = 0;
            }
        }

        return stepSize;
    }
    
    static FASTInputByteArray buildInputForTestingByteArray(File fileSource) {
        byte[] fileData = null;
        try {
            // do not want to time file access so copy file to memory
            fileData = new byte[(int) fileSource.length()];
            FileInputStream inputStream = new FileInputStream(fileSource);
            int readBytes = inputStream.read(fileData);
            inputStream.close();
            assert(fileData.length==readBytes);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        FASTInputByteArray fastInput = new FASTInputByteArray(fileData);
        return fastInput;
    }
    
    private static InputStream testDataInputStream(String resource) {
        
        InputStream resourceInput = Test.class.getResourceAsStream(resource);
        if (null!=resourceInput) {
            return resourceInput;            
        }
        
        try {
            return new FileInputStream(new File(resource));
        } catch (FileNotFoundException e) {
            throw new FASTException(e);
        }
    }

    private static byte[] buildRawCatalogData(ClientConfig clientConfig, String source) {


        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, source, clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert(catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }

}
