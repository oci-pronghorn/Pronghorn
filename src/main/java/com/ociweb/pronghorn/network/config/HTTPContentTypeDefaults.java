package com.ociweb.pronghorn.network.config;

public enum HTTPContentTypeDefaults implements HTTPContentType {

    //TODO: user smaller hash and put the lest used values at the bottom so they get the collision jump
    UNKNOWN("","",true),//MUST BE FIRST WITH ORDINAL ZERO, market as alias to ensrue its not used for file lookup.
    AI("application/postscript","ai"),
    AIF("audio/x-aiff","aif"),
    AIFF("audio/x-aiff","aiff"),
    AIFC("audio/x-aiff","aifc"),
    AU("audio/basic", "au"),
    AVI("video/avi","avi"),
    AVIMSX("video/x-msvideo","avi",true),
    AVIMS("video/msvideo","avi",true),
    BIN("application/octet-stream","bin"),
    BMP("image/bmp","bmp"),
    BZ2("application/x-bzip2","bz2"),
    CGI("application/x-httpd-cgi","cgi"),
    CLASS("application/octet-stream","class"),
    CPIO("application/x-cpio","cpio"),
    CSH("application/x-csh","csh"),    
    CSS("text/css","css"),
    DMS("application/octet-stream","dms"),
    DTD("application/xml-dtd","dtd"),
    DOC("application/msword","doc"),
    DVI("application/x-dvi","dvi"),
    EPS("application/postscript","eps"),
    EXE("application/octet-stream","exe"),
    ETX("text/x-setext","etx"),
    GIF("image/gif","gif"),
    GTAR("application/x-gtar","gtar"),
    GZ("application/x-gzip","gz"),
    IEF("image/ief","ief"),
    HDF("application/x-hdf","hdf"),
    HQX("application/mac-binhex40","hqx"),
    HTML("text/html","html"),    
    HTM("text/html","htm"),   
    JAR("application/java-archive","jar"),
    JPG("image/jpeg","jpg"),
    JPEG("image/jpeg","jpeg"),
    JPE("image/jpeg","jpe"),    
    JSON("application/json","json"),
    JS("application/x-javascript","js"),
    FORM("application/x-www-form-urlencoded","form"),
    LATEX("application/x-latex","latex"),
    LHA("application/octet-stream","lha"),
    LZH("application/octet-stream","lzh"),
    MIDI("audio/x-midi","midi"),
    MOV("video/quicktime","mov"),
    MOVIE("video/x-sgi-movie","movie"),
    WRL("x-world/x-vrml","wrl"),
    VRML("x-world/x-vrml","vrml"),
    MP2("audio/mpeg","mp2"),
    MP3("audio/mpeg","mp3"),
    MPEG("video/mpeg","mpeg"),
    MPG("video/mpeg","mpg"),
    MPE("video/mpeg","mpe"),
    MPGA("audio/mpeg","mpga"),
    OGGVORB("audio/vorbis","ogg",true),
    OGG("application/ogg","ogg"),    
    PDF("application/pdf","pdf"),
    PL("application/x-perl","pl"),    
    PNG("image/png","png"),
    PNM("image/x-portable-anymap","pnm"),
    PBM("image/x-portable-bitmap","pbm"),
    PGM("image/x-portable-graymap","pgm"),
    PPM("image/x-portable-pixmap","ppm"),
    RGB("image/x-rgb","rgb"),
    XBM("image/x-xbitmap","xbm"),
    XPM("image/x-xpixmap","xpm"),
    XWD("image/x-xwindowdump","xwd"),
    PPT("application/vnd.ms-powerpoint","ppt"),
    PS("application/postscript","ps"),
    QT("video/quicktime","qt"),
    RAX("audio/x-pn-realaudio","ra",true),
    RA("audio/vnd.rn-realaudio","ra"),    
    RAMX("audio/x-pn-realaudio","ram",true),
    RAM("audio/vnd.rn-realaudio","ram"),    
    RAS("image/x-cmu-raster","ras"),
    RDF("application/rdf","rdf"),
    RDFXML("application/rdf+xml","rdf",true),    
    RTF("application/rtf","rtf"),
    RTX("text/richtext","rtx"),
    SGML("text/sgml","sgml"),
    SGM("text/sgml","sgm"),
    SIT("application/x-stuffit","sit"),
    SND("audio/basic","snd"),
    SVG("image/svg+xml","svg"),
    SWF("application/x-shockwave-flash","swf"),
    TAR("application/x-tar","tar"),
    //TAR_GZ("application/x-tar","tar.gz"), //may not be a good idea, test this. NOTE: if needed support this as a special case conditional
    TCL("application/x-tcl","tcl"),
    TGZ("application/x-tar","tgz"),
    TIFF("image/tiff","tiff"),
    TIF("image/tiff","tif"),
    TSV("text/tab-separated-values","tsv"),
    TXT("text/plain","txt"),
    VCD("application/x-cdlink","vcd"),
    WAV("audio/wav","wav"),
    WAVX("audio/x-wav","wav",true),    
    XLS("application/vnd.ms-excel","xls"),
    XML("application/xml","xml"),
    Z("application/x-compress","Z"),  
    ZIPX("application/x-compressed-zip","zip",true),
    ZIP("application/zip","zip");
    
    
    private final String contentType;
    private final String fileExtension;
    private final boolean isAlias; //non alias entries are the only ones used for type lookup based on file extension.
    private final byte[] bytes;
    
    private HTTPContentTypeDefaults(String contentType, String fileExtension) {

    	int i = contentType.length();
    	while (--i>=0) {
    		
    		if (Character.isUpperCase(contentType.charAt(i))) {
    			throw new UnsupportedOperationException("all content types must be lower case. Fix: "+contentType);
    		}
    		
    	}
    	
    	//System.err.println("loading: "+contentType);
    	
        this.contentType = contentType;
        this.fileExtension = fileExtension;
        this.isAlias = false;
        this.bytes = contentType.getBytes();
    }

    private HTTPContentTypeDefaults(String contentType, String fileExtension, boolean isAlias) {

        this.contentType = contentType;
        this.fileExtension = fileExtension;
        this.isAlias = isAlias;
        this.bytes = contentType.getBytes();
    }

    @Override
    public byte[] getBytes() {
    	return bytes;
    }
    
    @Override
    public String contentType() {
        return contentType;
    }


    @Override
    public String fileExtension() {
        return fileExtension;
    }
    
    public boolean isAlias() {
        return isAlias;
    }
    
    
    
}
