package com.ociweb.jfast.generator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import com.ociweb.jfast.error.FASTException;


    public class FASTDispatchClassLoader extends ClassLoader{

        public static final String READER = "generated.FASTReaderDispatch";
        public static final String WRITER = "generated.FASTWriterDispatch";

        public FASTDispatchClassLoader(ClassLoader parent) {
            super(parent);
        }

        public Class loadClass(String name, InputStream input) throws ClassNotFoundException {
            if(!READER.equals(name) &&
               !WRITER.equals(name)) {
                    return super.loadClass(name);
            }
            
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                int data = input.read();

                while(data != -1){
                    buffer.write(data);
                    data = input.read();
                }

                input.close();

                byte[] classData = buffer.toByteArray();

                return defineClass(name, classData, 0, classData.length);

            } catch (Exception e) {
                throw new FASTException(e);
            }
        }
        
        
        

    }