//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.stream.FASTRingBuffer;

public class TemplateCatalog {

    public static final String KEY_PARAM_PREAMBLE_BYTES = "jFAST.preamble.bytes";
    
    // because optional values are sent as +1 when >= 0 it is not possible to
    // send the
    // largest supported positive value, as a result this is the ideal default
    // because it
    // can not possibly collide with any real values
    public static final int DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT = Integer.MAX_VALUE;
    public static final long DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG = Long.MAX_VALUE;

    final DictionaryFactory dictionaryFactory;
    final int maxTemplatePMapSize;
    final int maxNonTemplatePMapSize;
    final int maxPMapDepth;
    final int maxFieldId;

    public final int[] templateStartIdx; // TODO: X, these two arrays can be
                                         // shortened!
    public final int[] templateLimitIdx;

    public final int[] scriptTokens;
    final int[] scriptFieldIds;
    final String[] scriptFieldNames;
    final int templatesInCatalog;

    final Properties properties;
    
    final int[][] dictionaryMembers;

    // Runtime specific message prefix, only used for some transmission
    // technologies
    
    //TODO: A, move these into properties to be set on save only.
    int maxTextLength = 15;// default
    int maxByteVectorLength = 16;// default
    int textLengthGap = 2;//8;// default
    int byteVectorGap = 8;// default

    public static final int END_OF_SEQ_ENTRY = 0x01;
    public static final int END_OF_MESSAGE = 0x02;

    final FASTRingBuffer[] ringBuffers;
    
    public TemplateCatalog(byte[] catBytes) {
        
        FASTInputStream inputStream;
        try {
            inputStream = new FASTInputStream(new GZIPInputStream(new ByteArrayInputStream(catBytes),catBytes.length));
        } catch (IOException e) {
            throw new FASTException(e);
        }
        
        
        PrimitiveReader reader = new PrimitiveReader(1024, inputStream, 0);

        properties = new Properties();
        loadProperties(reader);
        
        int templatePow = PrimitiveReader.readIntegerUnsigned(reader);
        assert (templatePow < 32) : "Corrupt catalog file";
        templateStartIdx = new int[1 << templatePow];
        templateLimitIdx = new int[1 << templatePow];

        int fullScriptLength = PrimitiveReader.readIntegerUnsigned(reader);
        scriptTokens = new int[fullScriptLength];
        scriptFieldIds = new int[fullScriptLength];
        scriptFieldNames = new String[fullScriptLength];
        templatesInCatalog = PrimitiveReader.readIntegerUnsigned(reader);

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

        ringBuffers = buildRingBuffers(dictionaryFactory,fullScriptLength);
    }
    
    @Deprecated //for testing only
    public TemplateCatalog(DictionaryFactory dcr, int nonTemplatePMapSize, int[][] dictionaryMembers,
                          int[] fullScript, int maxNestedGroupDepth,
                          int primaryRingBits, int textRingBits, int stackPMapInBytes, int preambleSize) {
        
        this.scriptTokens = fullScript;
        this.maxNonTemplatePMapSize  = nonTemplatePMapSize;
        this.dictionaryMembers = dictionaryMembers;
        this.maxPMapDepth = maxNestedGroupDepth;
        this.templatesInCatalog=-1;
        this.templateStartIdx=null;
        this.templateLimitIdx=null;
        this.scriptFieldNames=null;
        this.scriptFieldIds=null;
        this.properties = new Properties();
        this.maxTemplatePMapSize = stackPMapInBytes;
        this.maxFieldId=-1;
        this.dictionaryFactory = dcr;
        int fullScriptLength = null==fullScript?1:fullScript.length;
        this.ringBuffers = buildRingBuffers(dictionaryFactory,fullScriptLength);
    }
    
    
    private static FASTRingBuffer[] buildRingBuffers(DictionaryFactory dFactory, int length) {
        FASTRingBuffer[] buffers = new FASTRingBuffer[length];
        //TODO: simple imlementation needs adavanced controls.
        FASTRingBuffer rb = new FASTRingBuffer((byte)8,(byte)7,dFactory, 10); //TODO: A, must compute max frag depth in template parser.    
        int i = length;
        while (--i>=0) {
            buffers[i]=rb;            
        }        
        return buffers;
        
        //TODO: B, build  null ring buffer to drop messages.
    }
    
    public FASTRingBuffer[] ringBuffers() {
        return ringBuffers;
    }
    
    
    private void loadProperties(PrimitiveReader reader) {
        int props = PrimitiveReader.readIntegerUnsigned(reader);
        StringBuilder builder = new StringBuilder();
        int len;
        while(--props>=0) {
            
            builder.setLength(0);
            len = PrimitiveReader.readIntegerUnsigned(reader);
            String key = PrimitiveReader.readTextUTF8(len, builder, reader).toString();
            
            builder.setLength(0);
            len = PrimitiveReader.readIntegerUnsigned(reader);
            String value = PrimitiveReader.readTextUTF8(len, builder, reader).toString();
            
            properties.put(key, value);
        }
    }


    // Assumes that the tokens are already loaded and ready for use.
    private void loadTemplateScripts(PrimitiveReader reader) {

        int i = templatesInCatalog;
        while (--i >= 0) {
            // look up for script index given the templateId
            int templateId = PrimitiveReader.readIntegerUnsigned(reader);
            templateStartIdx[templateId] = PrimitiveReader.readIntegerUnsigned(reader);
            templateLimitIdx[templateId] = PrimitiveReader.readIntegerUnsigned(reader);
            // System.err.println("templateId "+templateId);
        }
        // System.err.println("total:"+templatesInCatalog);

        StringBuilder builder = new StringBuilder();
        i = scriptTokens.length;
        while (--i >= 0) {
            scriptTokens[i] = PrimitiveReader.readIntegerSigned(reader);
            scriptFieldIds[i] = PrimitiveReader.readIntegerUnsigned(reader);
            int len = PrimitiveReader.readIntegerUnsigned(reader);
            String name ="";
            if (len>0) {
                builder.setLength(0);
                name = PrimitiveReader.readTextUTF8(len, builder, reader).toString();
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

    public static void save(PrimitiveWriter writer, int uniqueIds, int biggestId, int uniqueTemplateIds,
            int biggestTemplateId, DictionaryFactory df, int maxTemplatePMap, int maxNonTemplatePMap,
            int[][] tokenIdxMembers, int[] tokenIdxMemberHeads, int[] catalogScriptTokens, int[] catalogScriptFieldIds,
            String[] catalogScriptFieldNames, int scriptLength, int[] templateIdx, int[] templateLimit, int maxPMapDepth, Properties properties) {

        saveProperties(writer,properties);        
        
        saveTemplateScripts(writer, uniqueTemplateIds, biggestTemplateId, catalogScriptTokens, 
                catalogScriptFieldIds, catalogScriptFieldNames,
                scriptLength, templateIdx, templateLimit);

        saveDictionaryMembers(writer, tokenIdxMembers, tokenIdxMemberHeads);

        writer.writeIntegerUnsigned(biggestId);
        // System.err.println("save pmap sizes "+maxTemplatePMap+" "+maxNonTemplatePMap);
        writer.writeIntegerUnsigned(maxTemplatePMap);
        writer.writeIntegerUnsigned(maxNonTemplatePMap);
        writer.writeIntegerUnsigned(maxPMapDepth);

        df.save(writer);

    }

    private static void saveProperties(PrimitiveWriter writer, Properties properties) {
                
        Set<String> keys = properties.stringPropertyNames();
        writer.writeIntegerUnsigned(keys.size());
        for(String key: keys) {
            writer.writeIntegerUnsigned(key.length());
            writer.writeTextUTF(key);
            
            String prop = properties.getProperty(key);
            writer.writeIntegerUnsigned(prop.length());
            writer.writeTextUTF(prop);
        }
        
    }

    private static void saveDictionaryMembers(PrimitiveWriter writer, int[][] tokenIdxMembers, int[] tokenIdxMemberHeads) {
        // save count of dictionaries
        int dictionaryCount = tokenIdxMembers.length;
        writer.writeIntegerUnsigned(dictionaryCount);
        //
        int d = dictionaryCount;
        while (--d >= 0) {
            int[] members = tokenIdxMembers[d];
            int h = tokenIdxMemberHeads[d];
            writer.writeIntegerUnsigned(h);// length of reset script (eg member
                                           // list)
            while (--h >= 0) {
                writer.writeIntegerSigned(members[h]);
            }
        }
    }

    private void loadDictionaryMembers(PrimitiveReader reader) {
        // //target int[][] dictionaryMembers
        int dictionaryCount = dictionaryMembers.length;
        int d = dictionaryCount;
        while (--d >= 0) {
            int h = PrimitiveReader.readIntegerUnsigned(reader);// length of reset script (eg
                                                 // member list)
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
    private static void saveTemplateScripts(PrimitiveWriter writer, int uniqueTemplateIds, int biggestTemplateId,
            int[] catalogScriptTokens, int[] catalogScriptFieldIds, String[] catalogScriptFieldNames, int scriptLength, int[] templateStartIdx,
            int[] templateLimitIdx) {
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
        writer.writeIntegerUnsigned(pow);// will be < 32
        writer.writeIntegerUnsigned(scriptLength);

        // total number of templates are are defining here in the catalog
        writer.writeIntegerUnsigned(uniqueTemplateIds);
        // write each template index
        int i = templateStartIdx.length;
        while (--i >= 0) {
            if (0 != templateStartIdx[i]) {
                writer.writeIntegerUnsigned(i);
                writer.writeIntegerUnsigned(templateStartIdx[i] - 1); // return
                                                                      // the
                                                                      // index
                                                                      // to its
                                                                      // original
                                                                      // value
                                                                      // (-1)
                writer.writeIntegerUnsigned(templateLimitIdx[i]);
            }
        }

        // write the scripts
        i = scriptLength;
        while (--i >= 0) {
            writer.writeIntegerSigned(catalogScriptTokens[i]);
            writer.writeIntegerUnsigned(catalogScriptFieldIds[i]); 
            String name = catalogScriptFieldNames[i];
            int len = null==name?0:name.length();
            writer.writeIntegerUnsigned(len);
            if (len>0) {
                writer.writeTextUTF(name);
            }
        }

    }

    public DictionaryFactory dictionaryFactory() {
        return dictionaryFactory;
    }

    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }
    
    public int getIntProperty(String key, int absentValue) {
        return properties.containsKey(key)?Integer.parseInt(properties.getProperty(key)):absentValue;
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

    public int getMaxTextLength() {
        return maxTextLength;
    }

    public int getMaxByteVectorLength() {
        return maxByteVectorLength;
    }

    public int templatesCount() {
        return templatesInCatalog;
    }

    public int[] fullScript() {
        return scriptTokens;
    }
    public int[] fieldIdScript() {
        return scriptFieldIds;
    }
    public String[] fieldNameScript() {
        return scriptFieldNames;
    }

    public int getByteVectorGap() {
        return byteVectorGap;
    }

    public int getTextGap() {
        return textLengthGap;
    }

    public int maxNonTemplatePMapSize() {
        return maxNonTemplatePMapSize;
    }

    public int getMaxGroupDepth() {
        return maxPMapDepth;
    }

}
