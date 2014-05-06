package com.ociweb.jfast.loader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;

import com.ociweb.jfast.stream.FASTReaderScriptPlayerDispatch;

public class SourceTemplates {

    String templateText;

    public String getRawSource() {
        if (null==templateText) {
            templateText = templateSource();
        }
        return templateText;
    }
    
    private String templateSource() {
        
         URL sourceData = getClass().getResource("/FASTReaderDispatch.java");
         File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        //File sourceDataFile = new File("/home/nate/SpiderOak Hive/kepler/jFAST/src/main/java/com/ociweb/jfast/stream/FASTReaderDispatch.java");
                
        
        String templateSource = "";
        
        try {
            byte[] buffer = new byte[(int)sourceDataFile.length()];
            BufferedInputStream bist = new BufferedInputStream(new FileInputStream(sourceDataFile));
            bist.read(buffer);
            bist.close();
            templateSource = new String(buffer);
        
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return templateSource;
    }
    
    public String template(String methodName) {
        int idx = getRawSource().indexOf(methodName);
        //start from idx and find the first {
        int start = getRawSource().indexOf('{', idx)+1;
        while (getRawSource().charAt(start)=='\n' || getRawSource().charAt(start)=='\r') {
            start++;
        }
        //find matching }
        int depth = 1;
        int pos = start;
        while (depth>0) {
            if (getRawSource().charAt(pos)=='{') {
                depth++;
            }
            if (getRawSource().charAt(pos)=='}') {
                depth--;
            }
            pos++;
        }
        //pos is now right after closing } and we want to be before
        int stop = pos-1;
        //
        return getRawSource().substring(start,stop);
    }
    
    public String imports() { //TODO: A, pull in imports
        
        //the includes before this.
        //public class FASTReaderDispatch
        return "";
    }
    
    public String constructor() { //TODO: pull in constructor.
        
        //find this.
        //public FASTReaderDispatch(
        
        return "";
    }
    
    public String[] params(String methodName) {
        int idx = getRawSource().indexOf(methodName);
        //start from idx and find the first (
        int start = getRawSource().indexOf('(', idx)+1;
        int stop = getRawSource().indexOf(')',start);
        String[] para = getRawSource().substring(start, stop).split(",");
        //extractType
        int i = para.length;
        while (--i>=0) {
            para[i] = para[i].trim();
            para[i] = para[i].substring(para[i].indexOf(' ')+1).trim();
        }
        return para;
    }
    
    public String[] defs(String methodName) {
        int idx = getRawSource().indexOf(methodName);
        //start from idx and find the first (
        int start = getRawSource().indexOf('(', idx)+1;
        int stop = getRawSource().indexOf(')',start);
        String[] para = getRawSource().substring(start, stop).split(",");
        //extractType
        int i = para.length;
        while (--i>=0) {
            para[i] = para[i].trim();
            //////exactly the same as the params() method except for removing this line.
        }
        return para;
    }
    
    
}
