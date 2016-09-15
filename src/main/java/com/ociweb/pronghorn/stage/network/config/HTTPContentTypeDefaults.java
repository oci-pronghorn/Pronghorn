package com.ociweb.pronghorn.stage.network.config;

public enum HTTPContentTypeDefaults implements HTTPContentType {

    //TODO: user smaller hash and put the lest used values at the bottom so they get the collision jump
    UNKNOWN("",""),//MUST BE FIRST WITH ORDINAL ZERO
    AI("application/postscript","ai"),
    AIF("audio/x-aiff","aif"),
    AIFF("audio/x-aiff","aiff"),
    AIFC("audio/x-aiff","aifc"),
    AU("audio/basic", "au"),
    AVI("video/avi","avi"),
    AVIMSX("video/x-msvideo","avi"),
    AVIMS("video/msvideo","avi"),
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
    JS("application/x-javascript","js"),
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
    OGGVORB("audio/vorbis","ogg"),
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
    RAX("audio/x-pn-realaudio","ra"),
    RA("audio/vnd.rn-realaudio","ra"),    
    RAMX("audio/x-pn-realaudio","ram"),
    RAM("audio/vnd.rn-realaudio","ram"),    
    RAS("image/x-cmu-raster","ras"),
    RDF("application/rdf","rdf"),
    RDFXML("application/rdf+xml","rdf"),    
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
    WAVX("audio/x-wav","wav"),    
    XLS("pplication/vnd.ms-excel","xls"),
    XML("application/xml","xml"),
    Z("application/x-compress","Z"),  
    ZIPX("application/x-compressed-zip","zip"),
    ZIP("application/zip","zip");
    
    
    private final String contentType;
    private final String fileExtension;
    
    
    private HTTPContentTypeDefaults(String contentType, String fileExtension) {

        this.contentType = contentType;
        this.fileExtension = fileExtension;

    }


    @Override
    public String contentType() {
        return contentType;
    }


    @Override
    public String fileExtension() {
        return fileExtension;
    }
    
    
    
    
}
