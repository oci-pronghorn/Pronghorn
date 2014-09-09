package com.ociweb.jfast.catalog.extraction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.ociweb.jfast.catalog.generator.CatalogGenerator;
import com.ociweb.jfast.catalog.generator.FieldGenerator;
import com.ociweb.jfast.catalog.generator.ItemGenerator;
import com.ociweb.jfast.catalog.generator.TemplateGenerator;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateHandler;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;

public class TypeTrie {

    //High                       1
    private static final int BITS_DECIMAL = 11; //this is the only one that keeps count
    private static final int BITS_SIGN    =    3;
    private static final int BITS_DOT     =      3;
    private static final int BITS_COMMA   =        3;
    private static final int BITS_ASCII   =          3;
    private static final int BITS_OTHER   =            3;   
    
    private static final int SHIFT_OTHER   = 0;
    private static final int SHIFT_ASCII   = BITS_OTHER+SHIFT_OTHER;
    private static final int SHIFT_COMMA   = BITS_ASCII+SHIFT_ASCII;
    private static final int SHIFT_DOT     = BITS_COMMA+SHIFT_COMMA;
    private static final int SHIFT_SIGN    = BITS_DOT+SHIFT_DOT;
    private static final int SHIFT_DECIMAL = BITS_SIGN+SHIFT_SIGN;
    
    private static final int SATURATION_MASK = (1 << (BITS_SIGN + BITS_DOT + BITS_COMMA + BITS_ASCII + BITS_OTHER))-1;
    
        
    private static final int ONE_DECIMAL = 1<<SHIFT_DECIMAL; //0010 0000 0000 0000
    private static final int ONE_SIGN    = 1<<SHIFT_SIGN;    //0000 0100 0000 0000
    private static final int ONE_DOT     = 1<<SHIFT_DOT;     //0000 0000 1000 0000
    private static final int ONE_COMMA   = 1<<SHIFT_COMMA;   //0000 0000 0001 0000
    private static final int ONE_ASCII   = 1<<SHIFT_ASCII;   //0000 0000 0000 0100
    private static final int ONE_OTHER   = 1<<SHIFT_OTHER;   //0000 0000 0000 0001
               
    private static final int   ACCUM_MASK = (((1<<(BITS_DECIMAL-1))-1)<<SHIFT_DECIMAL) |
                                    (((1<<(BITS_SIGN-1))-1)<<SHIFT_SIGN) |
                                    (((1<<(BITS_DOT-1))-1)<<SHIFT_DOT) |
                                    (((1<<(BITS_COMMA-1))-1)<<SHIFT_COMMA) |
                                    (((1<<(BITS_ASCII-1))-1)<<SHIFT_ASCII) |
                                    (((1<<(BITS_OTHER-1))-1)<<SHIFT_OTHER);
    
    private static final int TYPE_UINT = TypeMask.IntegerUnsigned>>1; //  0
    private static final int TYPE_SINT = TypeMask.IntegerSigned>>1;   //  1
    private static final int TYPE_ULONG = TypeMask.LongUnsigned>>1;   //  2
    private static final int TYPE_SLONG = TypeMask.LongSigned>>1;     //  3
    private static final int TYPE_ASCII = TypeMask.TextASCII>>1;      //  4 
    private static final int TYPE_BYTES = TypeMask.TextUTF8>>1;       //  5 
    private static final int TYPE_DECIMAL = TypeMask.Decimal>>1;      //  6
    private static final int TYPE_NULL = 7;//no need to use BYTE_ARRAY, its the same as UTF8
    //NOTE: more will be added here for group and sequence once JSON support is added
    private static final int TYPE_EOM = 15;
    
    
    private final int[] accumValues;
    
    //TODO: need to trim leading white space for decision
    //TODO: need to keep leading real char for '0' '+' '-'
    
    //TODO: null will be mapped to default bytearray null?
    //TODO: next step stream this data using next visitor into the ring buffer and these types.

    //Need flag to turn on that feature
    ///TODO: convert short byte sequences to int or long
    ///TODO: treat leading zero as ascii not numeric.
    
    private int         activeSum;
    private int         activeLength;
    private boolean     activeQuote;
   
    
    //16 possible field types
    
    private static final int   typeTrieUnit = 16;
    private static final int   typeTrieSize = 1<<20; //1M
    private static final int   typeTrieMask = typeTrieSize-1;
    
    private static final int   OPTIONAL_SHIFT = 30;
    private static final int   OPTIONAL_FLAG  = 1<<OPTIONAL_SHIFT;
    private static final int   CATALOG_TEMPLATE_ID = 0;
    
    private final int[] typeTrie = new int[typeTrieSize];
    private int         typeTrieCursor; 
    private int         typeTrieLimit = typeTrieUnit;
    
    byte[] catBytes;
    
    public TypeTrie() {
        //one value for each of the possible bytes we may encounter.
        accumValues = new int[256];
        int i = 256;
        while (--i>=0) {
            accumValues[i] = ONE_OTHER;
        }
        i = 127;
        while (--i>=0) {
            accumValues[i] = ONE_ASCII;
        }
        i = 58;
        while (--i>=48) {
            accumValues[i] = ONE_DECIMAL;
        }
        accumValues[(int)'+'] = ONE_SIGN;
        accumValues[(int)'-'] = ONE_SIGN;        
        accumValues[(int)'.'] = ONE_DOT;
        accumValues[(int)','] = ONE_COMMA; //required when comma is not the delimiter to support thousands marking in US english
                
        
        resetFieldSum();
        restToRecordStart();
        
    }
    
    MappedByteBuffer tempBuffer;
    int tempPos;
    
    public void appendContent(MappedByteBuffer mappedBuffer, int pos, int limit, boolean contentQuoted) {
        
        tempBuffer = mappedBuffer;
        tempPos = pos;
        
        activeQuote |= contentQuoted;
        
        int i = (limit-pos);
        activeLength+=i;
        while (--i>=0) {
            byte b = mappedBuffer.get(pos+i);
            int x = activeSum + accumValues[0xFF&b];            
            activeSum = (x|(((x>>1) & ACCUM_MASK) & SATURATION_MASK))&ACCUM_MASK;                     
        };        
        
    }
    
    public void appendNewRecord() {
        
        //for debug
        if (431==(typeTrieCursor+TYPE_EOM)) {
            
            // need junk filter up front in parser? escape sequces?
            // Fair, Isaac, Inco     bulk repkace??
            // Charming Shoppes,,,   bulk replace??
            
            byte[] dst = new byte[200];
            ByteBuffer x = tempBuffer.asReadOnlyBuffer();
            x.position(tempPos-100);
            
            x.get(dst, 0, dst.length);
            System.err.println("<"+new String(dst)+">");
            
        }
        
        typeTrie[typeTrieCursor+TYPE_EOM]++;        
        restToRecordStart();
        
    }
    
    public void appendNewField() {
        int type = extractType();
        
        //store type into the Trie to build messages.
        int pos = typeTrieCursor+type;
        if (typeTrie[pos]==0) {
            //create new position          
            typeTrieCursor = typeTrie[pos] = typeTrieLimit;
            typeTrieLimit += typeTrieUnit;
        } else {
            typeTrieCursor = typeTrieMask&typeTrie[pos];
        }
    }
    
    public int moveNextField() {
        int type = extractType();        
        int pos = typeTrieCursor+type;
        typeTrieCursor = typeTrieMask&typeTrie[pos];  
        return type;
    }

    private int extractType() {
        assert(activeLength>=0);
                
        //split fields.
        int otherCount = ((1<<BITS_OTHER)-1)&(activeSum>>SHIFT_OTHER);
        int decimalCount = ((1<<BITS_DECIMAL)-1)&(activeSum>>SHIFT_DECIMAL);
        int asciiCount = ((1<<BITS_ASCII)-1)&(activeSum>>SHIFT_ASCII);
        int signCount = ((1<<BITS_SIGN)-1)&(activeSum>>SHIFT_SIGN);
        
        //NOTE: swap these two assignments for British vs American numbers
        int dotCount = ((1<<BITS_DOT)-1)&(activeSum>>SHIFT_DOT);
        int commaCount = ((1<<BITS_COMMA)-1)&(activeSum>>SHIFT_COMMA);
        
        if (commaCount>0) {
            System.err.println("did not expect any commas");
        }                     
        
        //apply rules to determine field type
        int type;
        if (activeLength==0 || activeSum==0) {
            //null field
            type = TYPE_NULL; 
        } else {        
            if (otherCount>0) {
                //utf8 or byte array
                type = TYPE_BYTES;
            } else { 
                if (asciiCount>0 || activeQuote || signCount>1 || dotCount>1 || decimalCount>18) { //NOTE: 18 could be optimized by reading first digit
                    //ascii text
                    type = TYPE_ASCII;
                } else {
                    if (dotCount==1) {
                        //decimal 
                        type = TYPE_DECIMAL;                        
                    } else {  
                        //no dot
                        if (decimalCount>9) { //NOTE: 9 could be optimized by reading first digit
                            //long
                            if (signCount==0) {
                                //unsigned
                                type = TYPE_ULONG;
                            } else {
                                //signed
                                type = TYPE_SLONG;
                            }
                        } else {
                            //int
                            if (signCount==0) {
                                //unsigned
                                type = TYPE_UINT; //TODO: may need to bump up to long if that is all we find when building records.
                            } else {
                                //signed
                                type = TYPE_SINT;
                            }                            
                        }
                    }                
                }           
            }
        }
        resetFieldSum();
        return type;
    }
    
    
    private void resetFieldSum() {
        activeLength = 0;
        activeSum = 0;
        activeQuote = false;
    }
    
    public void restToRecordStart() {
        typeTrieCursor=0;
    }
    
    
    //if all zero but the null then do recurse null otherwise not.
    
    public void printRecursiveReport(int pos, String tab) {
        
        int i = typeTrieUnit;
        boolean noOutput = true;
        while (--i>=0) {
            int value = typeTrieMask&typeTrie[pos+i];
            if (value > 0) {                
                if (i==TYPE_EOM) {
                        System.err.print(tab+"Count:"+value+" at "+(pos+i)+"\n");
                        noOutput = false;
                } else {
                    int type = i<<1;
                    if (type<TypeMask.methodTypeName.length) {                    
                        
                        
                        String v = (i==TYPE_NULL ? "NULL" : TypeMask.methodTypeName[type]);
                        
                        if ((OPTIONAL_FLAG&typeTrie[pos+i])!=0) {
                            v = "Optional"+v;
                        }
                        noOutput = false;
                        
                        System.err.println(tab+v);
                        printRecursiveReport(value, tab+"     ");    
                    }
                }        
            }
        }        
        if (noOutput) {
            System.err.print(tab+"Count:ZERO\n");
        }
    }
    

    
    void mergeOptionalNulls(int pos) {
        
        int i = typeTrieUnit;
        while (--i>=0) {
            int value = typeTrieMask&typeTrie[pos+i];
            if (value > 0) {                
                if (i!=TYPE_EOM) {
                    mergeOptionalNulls(value);
    
                    //finished call for this position i so it can removed if needed
                    //after merge on the way up also ensure we are doing the smallest parts first then larger ones
                    //and everything after this point is already merged.
                    
                    int lastNonNull = lastNonNull(pos, typeTrie, typeTrieUnit);
                    
                    if (lastNonNull>=0 && TYPE_NULL==i) {
                    
                       // System.err.println("found one "+lastNonNull);
                    
                        //check if there is another non-null field
                        //if there is more than 1 field with the null NEVER collapse because we don't know which path to which it belongs.
                        //we will produce 3 or more separate templates and they will be resolved by the later consumption stages
                        if (lastNonNull(pos,typeTrie,lastNonNull)<0) {
                            
                            int nullPos = value;
                            int thatPosDoes = typeTrieMask&typeTrie[pos+lastNonNull];
                            
                            //if recursively all of null pos is contained in that pos then we will move it over.                            
                            if (contains(nullPos,thatPosDoes,0)) {
                                //since the null is a full subset add all its counts to the rlarger
                                sum(nullPos,thatPosDoes);                                
                                //flag this type as optional
                                typeTrie[pos+lastNonNull] |= OPTIONAL_FLAG;
                                //delete old null branch
                                typeTrie[pos+i] = 0;
                            }
                        }
                    }
                }        
            }            
        }        
    }
    
    void mergeIntLongs(int pos) {
        
        int i = typeTrieUnit;
        while (--i>=0) {
            int value = typeTrieMask&typeTrie[pos+i];
            if (value > 0) {                
                if (i!=TYPE_EOM) {
                    mergeIntLongs(value);
    
                    //finished call for this position i so it can removed if needed
                    //after merge on the way up also ensure we are doing the smallest parts first then larger ones
                    //and everything after this point is already merged.

                    mergeTypes(pos, i, value, TYPE_ULONG, TYPE_UINT);
                    mergeTypes(pos, i, value, TYPE_SLONG, TYPE_SINT);
                    
                    
                    
                }        
            }            
        }        
    }

    private void mergeTypes(int pos, int i, int value, int t1, int t2) {
        int thatPosDoes = typeTrieMask&typeTrie[pos+t1];
        if (thatPosDoes>0 && t2==i) {
                                   
                
                //if recursively all of null pos is contained in that pos then we will move it over.                            
                if (contains(value,thatPosDoes,0)) {
                    //since the null is a full subset add all its counts to the rlarger
                    sum(value,thatPosDoes);               
                    //delete old UINT branch
                    typeTrie[pos+i] = 0;
                }

        }
    }
    

    private boolean contains(int subset, int targetset, int depth) {
        //if all the field in inner are contained in outer
        int i = typeTrieUnit;
        while (--i>=0) {
            //exclude this type its only holding the count
            if (TYPE_EOM!=i) {
                if (0!=(typeTrieMask&typeTrie[subset+i])) { 
                    if (0==(typeTrieMask&typeTrie[targetset+i])) {
                        return false;
                    }
                    
                    if (!contains(typeTrieMask&typeTrie[subset+i],typeTrieMask&typeTrie[targetset+i],depth+1)  ) {
                        return false;
                    }
                }
            }
        }                    
        return true;
    }
    
    private boolean sum(int subset, int targetset) {
        //if all the field in inner are contained in outer
        int i = typeTrieUnit;
        while (--i>=0) {
            //exclude this type its only holding the count
            if (TYPE_EOM!=i) {
                if (0!=(typeTrieMask&typeTrie[subset+i])) {
                    if (0==(typeTrieMask&typeTrie[targetset+i]) || !sum(typeTrieMask&typeTrie[subset+i],typeTrieMask&typeTrie[targetset+i])  ) {
                        return false;
                    }
                }
                //don't loose the optional flag from the other branch if it is there
                typeTrie[targetset+i] |= (OPTIONAL_FLAG & typeTrie[subset+i]);
            } else {
                //everything matches to this point so add the inner into the outer
                typeTrie[targetset+i] += typeTrie[subset+i];
            }
        }                    
        return true;
    }
    
    
    private static int lastNonNull(int pos, int[] typeTrie, int startLimit) {
        int i = startLimit;
        while (--i>=0) {
            if (TYPE_NULL!=i) {
                if (0!=typeTrie[pos+i]) {
                    return i;
                }
            }            
        }
        return -1;
    }
    
    
    private  void catalog(int pos, StringBuilder target, ItemGenerator[] buffer, int idx) {
        
        int i = typeTrieUnit;
        while (--i>=0) {
            int raw = typeTrie[pos+i];
            int value = typeTrieMask&raw;
            int optionalBit = 1&(raw>>OPTIONAL_SHIFT);
            
            if (value > 0) {                
                if (i==TYPE_EOM) {
                        
                    String name=""+pos;
                    int id=pos;
                    
                    boolean reset=false;
                    String dictionary=null;
                    
                    TemplateGenerator.openTemplate(target, name, id, reset, dictionary);
                    
                    int j = 0;
                    while (j<idx) {
                        buffer[j].appendTo("    ", target);
                        j++;
                    }
                        
                    TemplateGenerator.closeTemplate(target);
                    
                } else {
                    int type = i<<1;
                    if (type<TypeMask.methodTypeName.length) {        
                                                
                        if (i==TYPE_NULL) {
                            type = TypeMask.TextUTF8Optional;
                        } else {
                            type = type|optionalBit;
                            
                        }
                        
                        //pos + type is used because it will never collide
                        int id = pos+i; //pos skips by 16's so there is room for the i
                        String name = ""+id;
                        boolean presence = 1==optionalBit;
                        int operator = OperatorMask.Field_None;
                        String initial = null;
                        
                        buffer[idx] = new FieldGenerator(name,id,presence,type,operator,initial);                                      
                        
                        
                        catalog(value, target, buffer, idx+1);    
                    }
                }        
            }
        }        

    }
    
    
    public String buildCatalog(boolean withCatalogSupport) {
        
        StringBuilder target = new StringBuilder(1024);
        target.append(CatalogGenerator.HEADER);
               

        ItemGenerator[] buffer = new ItemGenerator[64];        
        catalog(0,target,buffer,0);
                
        if (withCatalogSupport) {
            addTemplateToHoldTemplates(target);
        }
        
        
        target.append(CatalogGenerator.FOOTER);
        return target.toString();      
        
        
    }

    public void addTemplateToHoldTemplates(StringBuilder target) {
        String name="catalog";
        int id=CATALOG_TEMPLATE_ID;
        
        boolean reset=true;
        String dictionary="global";
        
        TemplateGenerator.openTemplate(target, name, id, reset, dictionary);
        
        
        int catId = 100;
        String catName = ""+catId;
        boolean presence = false;
        int operator = OperatorMask.Field_None;
        String initial = null;
        int type = TypeMask.ByteArray;
        
        FieldGenerator fg = new FieldGenerator(catName,catId,presence,type,operator,initial);  
        fg.appendTo("    ", target);            
        
        TemplateGenerator.closeTemplate(target);
    }
    
    public byte[] catBytes(ClientConfig clientConfig) {
        String catalog = buildCatalog(true);
        
        clientConfig.setCatalogTemplateId(CATALOG_TEMPLATE_ID);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gZipOutputStream = new GZIPOutputStream(baos);
            FASTOutput output = new FASTOutputStream(gZipOutputStream);
            
            SAXParserFactory spfac = SAXParserFactory.newInstance();
            SAXParser sp = spfac.newSAXParser();
            InputStream stream = new ByteArrayInputStream(catalog.getBytes(StandardCharsets.UTF_8));           
            
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

    public void memoizeCatBytes() {
    //    System.err.println(buildCatalog(true));
        catBytes = catBytes(new ClientConfig());
        
    }
    
    public byte[] getCatBytes() {
        return catBytes;
    }
    
    
}
