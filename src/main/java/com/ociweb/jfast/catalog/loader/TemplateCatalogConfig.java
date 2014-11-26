//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.catalog.loader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.util.hash.IntHashTable;
import com.ociweb.pronghorn.ring.util.hash.IntHashTableVisitor;
import com.ociweb.jfast.stream.RingBuffers;

public class TemplateCatalogConfig {

    ///////////
    //properties that should only be set after reading the documentation.
    //Change impact generated code so the changes must be done only once before the catBytes are generated.
    ///////////
    
    public final ClientConfig clientConfig;
    
    
    // because optional values are sent as +1 when >= 0 it is not possible to
    // send the
    // largest supported positive value, as a result this is the ideal default
    // because it can not possibly collide with any real values
    public static final int DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT = Integer.MAX_VALUE;
    public static final long DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG = Long.MAX_VALUE;

    private final DictionaryFactory dictionaryFactory;
    private final int maxTemplatePMapSize;
    private final int maxNonTemplatePMapSize;
    private final int maxPMapDepth;
    private final int maxFieldId; 

    //these two arrays are as long as the biggest template id
    //if the template ids are sparse they may "waste" a bunch of space
    private final int[] templateStartIdx; 
    private final int[] templateLimitIdx;
    
    
    public final int[] templateScriptEntries;
    public final int[] templateScriptEntryLimits;

    final int[] scriptTokens;
    final int[] scriptFieldIds;
    private final String[] scriptFieldNames;
    private final int templatesInCatalog;

    
    private final int[][] dictionaryMembers;

    private final RingBuffers ringBuffers;

    private final FieldReferenceOffsetManager from;
    
    public TemplateCatalogConfig(byte[] catBytes) {
        
        FASTInputStream inputStream;
        try {
            inputStream = new FASTInputStream(new GZIPInputStream(new ByteArrayInputStream(catBytes),catBytes.length));
        } catch (IOException e) {
            throw new FASTException(e);
        }
        
        
        PrimitiveReader reader = new PrimitiveReader(1024, inputStream, 0);

                
        int templatePow = PrimitiveReader.readIntegerUnsigned(reader);
        assert (templatePow < 32) : "Corrupt catalog file";
        
        templateStartIdx = new int[1 << templatePow];  
        templateLimitIdx = new int[1 << templatePow];

        //given an index in the script lookup the tokens, fieldIds or fieldNames
        int fullScriptLength = PrimitiveReader.readIntegerUnsigned(reader);
        scriptTokens = new int[fullScriptLength];
        scriptFieldIds = new int[fullScriptLength];
        scriptFieldNames = new String[fullScriptLength];
        
        //given the template id from the template file look up the 
        //script starts and limits
        templatesInCatalog = PrimitiveReader.readIntegerUnsigned(reader);
        templateScriptEntries = new int[templatesInCatalog];
        templateScriptEntryLimits = new int[templatesInCatalog];
        
        loadTemplateScripts(reader);

        int dictionaryCount = PrimitiveReader.readIntegerUnsigned(reader);
        dictionaryMembers = new int[dictionaryCount][];

        loadDictionaryMembers(reader);

        maxFieldId = PrimitiveReader.readIntegerUnsigned(reader);
        // it is assumed that template PMaps are smaller or larger than the
        // other PMaps so these are kept separate
        maxTemplatePMapSize = PrimitiveReader.readIntegerUnsigned(reader);
        maxNonTemplatePMapSize = PrimitiveReader.readIntegerUnsigned(reader);
        maxPMapDepth = PrimitiveReader.readIntegerUnsigned(reader);

        dictionaryFactory = new DictionaryFactory(reader);
                
        clientConfig = new ClientConfig(reader);
        
        //must be done after the client config construction
        from = TemplateCatalogConfig
				.createFieldReferenceOffsetManager(this);
        ringBuffers = buildRingBuffers(dictionaryFactory, fullScriptLength, from, templateStartIdx, clientConfig);
        
    }
    
    @Deprecated //for testing only
    public TemplateCatalogConfig(DictionaryFactory dcr, int nonTemplatePMapSize, int[][] dictionaryMembers,
                                int[] fullScript, int maxNestedGroupDepth,
                                int maxTemplatePMapSize, int templatesCount, ClientConfig clientConfig) {
        
        this.scriptTokens = fullScript;
        this.maxNonTemplatePMapSize  = nonTemplatePMapSize;
        this.dictionaryMembers = dictionaryMembers;
        this.maxPMapDepth = maxNestedGroupDepth;
        this.templatesInCatalog=templatesCount;
        this.templateStartIdx=null;
        this.templateLimitIdx=null;
        this.scriptFieldNames=null;
        this.templateScriptEntries=null;
        this.templateScriptEntryLimits=null;
        this.scriptFieldIds=null;
        this.maxTemplatePMapSize = maxTemplatePMapSize;
        this.maxFieldId=-1;
        this.dictionaryFactory = dcr;
        int fullScriptLength = null==fullScript?1:fullScript.length;
        this.clientConfig = clientConfig;
        
        this.from = TemplateCatalogConfig.createFieldReferenceOffsetManager(this);
        
        this.ringBuffers = buildRingBuffers(dictionaryFactory,
                                            fullScriptLength, 
                                            from, templateStartIdx,
                                            clientConfig);
        
        //must be done after the client config construction
    }
    
    
    private static RingBuffers buildRingBuffers(DictionaryFactory dFactory, int scriptLength, 
                                                     FieldReferenceOffsetManager from, int[] templateStartIdx, 
                                                     ClientConfig clientConfig) {
        
        int primaryRingBits = clientConfig.getPrimaryRingBits(); 
        int textRingBits = clientConfig.getTextRingBits();
        
        RingBuffer[] buffers = new RingBuffer[scriptLength];
        //TODO: B, Same layout can be shared but every dispatch must have its OWN set of ring buffers, then for muxing the client will round robin. 1Producer to  1Consumer
        //Move this method into RingBuffers as satic?
        
        RingBuffer rb = new RingBuffer((byte)primaryRingBits,(byte)textRingBits,DictionaryFactory.initConstantByteArray(dFactory), from);
        int i = scriptLength;
        while (--i>=0) {
            buffers[i]=rb;            
        }        
        return new RingBuffers(buffers);
        
        //TODO: B, build  null ring buffer to drop messages.
    }
    
    public RingBuffers ringBuffers() {
        return ringBuffers;
    }
    
    

    // Assumes that the tokens are already loaded and ready for use.
    private void loadTemplateScripts(PrimitiveReader reader) {

        int i = templatesInCatalog;
        while (--i >= 0) {
            // look up for script index given the templateId
            int templateId = PrimitiveReader.readIntegerUnsigned(reader);
            templateScriptEntries[i] = templateStartIdx[templateId] = PrimitiveReader.readIntegerUnsigned(reader);
            templateScriptEntryLimits[i] = templateLimitIdx[templateId] = PrimitiveReader.readIntegerUnsigned(reader);
        }
        
        //Must be ordered in order to be useful 
        Arrays.sort(templateScriptEntries);
        Arrays.sort(templateScriptEntryLimits);

        StringBuilder builder = new StringBuilder();//TODO: B, this is now producing garbage! Temp space must be held by temp space owner!
        i = getScriptTokens().length;
        while (--i >= 0) {
            getScriptTokens()[i] = PrimitiveReader.readIntegerSigned(reader);
            scriptFieldIds[i] = PrimitiveReader.readIntegerUnsigned(reader);
            int len = PrimitiveReader.readIntegerUnsigned(reader);
            String name ="";
            if (len>0) {
                builder.setLength(0);
                {
                    byte[] tmp = new byte[len];                    
                    PrimitiveReader.readByteData(tmp,0,len,reader); //read bytes into array
                    
                    long charAndPos = 0;  //convert bytes to chars
                    while (charAndPos>>32 < len  ) { 
                        charAndPos = RingReader.decodeUTF8Fast(tmp, charAndPos, Integer.MAX_VALUE);
                        builder.append((char)charAndPos);

                    }
                }
                name = builder.toString();
            }
            scriptFieldNames[i] = name;
        }

        // System.err.println("script tokens/fields "+scriptTokens.length);//46
        // System.err.println("templateId idx start/stop count "+this.templateStartIdx.length);//128

    }

    // // stream message* | block*
    // // block BlockSize message+
    // // message segment
    // // segment PresenceMap TemplateIdentifier? (field | segment)*
    // //* field integer | string | delta | ScaledNumber | ByteVector
    // //* integer UnsignedInteger | SignedInteger
    // //* string ASCIIString | UnicodeString
    // //* delta IntegerDelta | ScaledNumberDelta | ASCIIStringDelta
    // |ByteVectorDelta

    public static void save(PrimitiveWriter writer, int biggestId, int uniqueTemplateIds, int biggestTemplateId,
            DictionaryFactory df, int maxTemplatePMap, int maxNonTemplatePMap, int[][] tokenIdxMembers,
            int[] tokenIdxMemberHeads, int[] catalogScriptTokens, int[] catalogScriptFieldIds, String[] catalogScriptFieldNames,
            int scriptLength,  IntHashTable templateToOffset, IntHashTable templateToLimit , int maxPMapDepth, ClientConfig clientConfig) {    
        
        saveTemplateScripts(writer, uniqueTemplateIds, biggestTemplateId, catalogScriptTokens, 
                catalogScriptFieldIds, catalogScriptFieldNames,
                scriptLength, templateToOffset, templateToLimit);

        saveDictionaryMembers(writer, tokenIdxMembers, tokenIdxMemberHeads);

        PrimitiveWriter.writeIntegerUnsigned(biggestId, writer);
        // System.err.println("save pmap sizes "+maxTemplatePMap+" "+maxNonTemplatePMap);
        PrimitiveWriter.writeIntegerUnsigned(maxTemplatePMap, writer);
        PrimitiveWriter.writeIntegerUnsigned(maxNonTemplatePMap, writer);
        PrimitiveWriter.writeIntegerUnsigned(maxPMapDepth, writer);

        df.save(writer);        
        clientConfig.save(writer);

    }

    private static void saveProperties(PrimitiveWriter writer, Properties properties) {
                
        Set<String> keys = properties.stringPropertyNames();
        PrimitiveWriter.writeIntegerUnsigned(keys.size(), writer);
        for(String key: keys) {
            PrimitiveWriter.writeIntegerUnsigned(key.length(), writer);
            PrimitiveWriter.ensureSpace(key.length(),writer);
            
            {
                //convert from chars to bytes
                //writeByteArrayData()
                int len = key.length();
                int limit = writer.limit;
                int c = 0;
                while (c < len) {
                    limit = RingReader.encodeSingleChar((int) key.charAt(c++), writer.buffer, limit);
                }
                writer.limit = limit;
            }
            
            String prop = properties.getProperty(key);
            PrimitiveWriter.writeIntegerUnsigned(prop.length(), writer);
            PrimitiveWriter.ensureSpace(prop.length(),writer);
            
            {
                //convert from chars to bytes
                //writeByteArrayData()
                int len = prop.length();
                int limit = writer.limit;
                int c = 0;
                while (c < len) {
                    limit = RingReader.encodeSingleChar((int) prop.charAt(c++), writer.buffer, limit);
                }
                writer.limit = limit;
            }
            
        }
        
    }

    private static void saveDictionaryMembers(PrimitiveWriter writer, int[][] tokenIdxMembers, int[] tokenIdxMemberHeads) {
        // save count of dictionaries
        int dictionaryCount = tokenIdxMembers.length;
        PrimitiveWriter.writeIntegerUnsigned(dictionaryCount, writer);
        //
        int d = dictionaryCount;
        while (--d >= 0) {
            int[] members = tokenIdxMembers[d];
            int h = tokenIdxMemberHeads[d];
            PrimitiveWriter.writeIntegerUnsigned(h, writer);// length of reset script (eg member list)
            while (--h >= 0) {
                PrimitiveWriter.writeIntegerSigned(members[h], writer);
            }
        }
    }

    private void loadDictionaryMembers(PrimitiveReader reader) {
        // //target int[][] dictionaryMembers
        int dictionaryCount = dictionaryMembers.length;
        int d = dictionaryCount;
        while (--d >= 0) {
            int h = PrimitiveReader.readIntegerUnsigned(reader);// length of reset script (eg member list)
            int[] members = new int[h];
            while (--h >= 0) {
                members[h] = PrimitiveReader.readIntegerSigned(reader);
            }
            dictionaryMembers[d] = members;
        }
    }

    /**
     * 
     * Save template scripts to the catalog file. The Script is made up of the
     * field id(s) or Tokens. Each field value needs to know the id so it is
     * stored by id. All other types (group tasks,dictionary tasks) just need to
     * be executed so they are stored as tokens only. These special tasks
     * frequently multiple tokens to a single id which requires that the token
     * is used in all cases. An example is the Open and Close tokens for a given
     * group.
     * 
     * 
     * @param writer
     * @param uniqueTemplateIds
     * @param biggestTemplateId
     * @param catalogScriptFieldNames 
     * @param scripts
     */
    private static void saveTemplateScripts(final PrimitiveWriter writer, int uniqueTemplateIds, int biggestTemplateId,
            int[] catalogScriptTokens, int[] catalogScriptFieldIds, String[] catalogScriptFieldNames, int scriptLength, 
            IntHashTable templateToOffset, final IntHashTable templateToLimit ) {
        // what size array will we need for template lookup. this must be a
        // power of two
        // therefore we will only store the exponent given a base of two.
        // this is not so much for making the file smaller but rather to do the
        // computation
        // now instead of at runtime when latency is an issue.
        int pow = 0;
        int tmp = biggestTemplateId;
        while (tmp != 0) {
            pow++;
            tmp = tmp >> 1;
        }
        assert (pow < 32);
        PrimitiveWriter.writeIntegerUnsigned(pow, writer);// will be < 32
        PrimitiveWriter.writeIntegerUnsigned(scriptLength, writer);

        // total number of templates are are defining here in the catalog
        PrimitiveWriter.writeIntegerUnsigned(uniqueTemplateIds, writer);
        // write each template index
        
        IntHashTable.visit(templateToOffset, new IntHashTableVisitor() {

			@Override
			public void visit(int key, int value) {
				
				PrimitiveWriter.writeIntegerUnsigned(key, writer);
				// return the index to its original value (-1)
                PrimitiveWriter.writeIntegerUnsigned(value - 1, writer);
                PrimitiveWriter.writeIntegerUnsigned(IntHashTable.getItem(templateToLimit, key), writer);
				
			}} );
               

        // write the scripts
        int i = scriptLength;
        while (--i >= 0) {
            PrimitiveWriter.writeIntegerSigned(catalogScriptTokens[i], writer);
            PrimitiveWriter.writeIntegerUnsigned(catalogScriptFieldIds[i], writer); 
            String name = catalogScriptFieldNames[i];
            
            int len = null==name?0:name.length();
            PrimitiveWriter.writeIntegerUnsigned(len, writer);
            if (len>0) {
                PrimitiveWriter.ensureSpace(name.length(),writer);
                
                //convert from chars to bytes
                //writeByteArrayData()
                int len1 = name.length();
                int limit = writer.limit;
                int c = 0;
                while (c < len1) {
                    limit = RingReader.encodeSingleChar((int) name.charAt(c++), writer.buffer, limit);
                }
                writer.limit = limit;
            }
        }

    }

    public DictionaryFactory dictionaryFactory() {
        return dictionaryFactory;
    }
   
    public int maxTemplatePMapSize() {
        return maxTemplatePMapSize;
    }

    public int maxFieldId() {
        return maxFieldId;
    }

    public int[][] dictionaryResetMembers() {
        return dictionaryMembers;
    }

    public ClientConfig clientConfig() {
        return clientConfig;
    }

    public int templatesCount() {
        return templatesInCatalog;
    }

    public int[] fullScript() {
        return getScriptTokens();
    }
    public int[] fieldIdScript() {
        return scriptFieldIds;
    }
    public String[] fieldNameScript() {
        return scriptFieldNames;
    }


    public int maxNonTemplatePMapSize() {
        return maxNonTemplatePMapSize;
    }

    public int getMaxGroupDepth() {
        return maxPMapDepth;
    }

    public int[] getTemplateStartIdx() {
        return templateStartIdx;
    }

    @Deprecated
    public int[] getTemplateLimitIdx() {
        return templateLimitIdx;
    }

    public int[] getScriptTokens() {
        return scriptTokens;
    }

    public FieldReferenceOffsetManager getFROM() {
        return from;
    }

    public static FieldReferenceOffsetManager createFieldReferenceOffsetManager(TemplateCatalogConfig config) {
		
		
		return new FieldReferenceOffsetManager(   config.scriptTokens, 
									        	  config.clientConfig.getPreableBytes(), 
									              config.fieldNameScript());
		
		
	}

	public static int maxPMapCountInBytes(TemplateCatalogConfig catalog) {
        return 2 + ((
                      catalog.maxTemplatePMapSize()>catalog.maxNonTemplatePMapSize() ?
                    		  catalog.maxTemplatePMapSize() + 2:
                    	      catalog.maxNonTemplatePMapSize() + 2) * catalog.getMaxGroupDepth());
    }

}
