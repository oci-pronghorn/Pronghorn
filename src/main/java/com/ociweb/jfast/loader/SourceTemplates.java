package com.ociweb.jfast.loader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import com.ociweb.jfast.stream.FASTReaderDispatch;

public class SourceTemplates {

    String source;
    
    public SourceTemplates(){
        source = templateSource();
        //System.err.println(source);
    }
    
    private String templateSource() {
        URL sourceData = getClass().getResource("/FASTReaderDispatch.java");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        
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
        
        return templateSource.substring(templateSource.indexOf(FASTReaderDispatch.START_HERE));
    }
    
    public String template(String methodName) {
        int idx = source.indexOf(methodName);
        //start from idx and find the first {
        int start = source.indexOf('{', idx)+1;
        while (source.charAt(start)=='\n' || source.charAt(start)=='\r') {
            start++;
        }
        //find matching }
        int depth = 1;
        int pos = start;
        while (depth>0) {
            if (source.charAt(pos)=='{') {
                depth++;
            }
            if (source.charAt(pos)=='}') {
                depth--;
            }
            pos++;
        }
        //pos is now right after closing } and we want to be before
        int stop = pos-1;
        //
        return source.substring(start,stop);
    }
    
    public String[] params(String methodName) {
        int idx = source.indexOf(methodName);
        //start from idx and find the first (
        int start = source.indexOf('(', idx)+1;
        int stop = source.indexOf(')',start);
        String[] para = source.substring(start, stop).split(",");
        //extractType
        int i = para.length;
        while (--i>=0) {
            para[i] = para[i].trim();
            para[i] = para[i].substring(para[i].indexOf(' ')+1).trim();
        }
        return para;
    }
    
    
}
