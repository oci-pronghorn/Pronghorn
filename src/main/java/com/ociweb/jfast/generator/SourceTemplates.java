package com.ociweb.jfast.generator;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;

public class SourceTemplates {

    String templateText;

    public String getRawSource() {
        if (null==templateText) {
            templateText = templateSource();
        }
        return templateText;
    }
    
    private String templateSource() {
        
        final String sourceFile = "/"+FASTReaderDispatchTemplates.class.getSimpleName()+".java";        
        InputStream inputStream = SourceTemplates.class.getResourceAsStream(sourceFile);
        if (null!=inputStream) {
           
            int v;
            StringBuilder builder = new StringBuilder();
            try {
                while ((v=inputStream.read())>0) {
                    builder.append((char)v);                    
                }
                inputStream.close();
            } catch (IOException e) {
                throw new FASTException(e);
            }
            return builder.toString();
            
        }

        //When we are doing active development the file will be found here
        //This allows for interactive testing without having to complete the full release cycle.
        
        File sourceDataFile = new File(readerDispatchTemplateSourcePath());
        if (!sourceDataFile.exists()) {
            //when we are in production the file will be found here
             URL sourceData = getClass().getResource(sourceFile);              
             try {
                sourceDataFile = new File(sourceData.toURI().toString());
            } catch (URISyntaxException e) {
                throw new FASTException(e);
            }
        }
        
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
    
    public String imports() {
        String source = getRawSource();
        return source.substring(source.indexOf("import"), 
                                source.indexOf("public abstract class "+FASTReaderDispatchTemplates.class.getSimpleName()));
    }
    
    public String constructor() {
        String source = getRawSource();
        int startIdx = source.indexOf("public "+FASTReaderDispatchTemplates.class.getSimpleName());
        return source.substring(startIdx,source.indexOf('}',startIdx)+1);
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

    public static String readerDispatchTemplateSourcePath() {
        File classFile;
        try {
            String name = FASTReaderDispatchTemplates.class.getSimpleName() + ".class";
            URL resource = FASTReaderDispatchTemplates.class.getResource(name);
            classFile = new File(resource.toURI());
            //assuming a maven directory structure the needed source file should be found here
            return classFile.getPath()
                    .replaceFirst("target.classes", "src"+File.separatorChar+"main"+File.separatorChar+"java")
                    .replace(".class",".java");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    
    
}
