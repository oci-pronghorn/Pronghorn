package com.ociweb.jfast.generator;

import com.ociweb.jfast.stream.FASTReaderDispatchBase;

public class LoadFASTDispatch {

    
    public void test() {
        
        ClassLoader parentClassLoader = FASTDispatchClassLoader.class.getClassLoader();
        FASTDispatchClassLoader classLoader = new FASTDispatchClassLoader(parentClassLoader);
        try {
            Class dispatchClass = classLoader.loadClass(FASTDispatchClassLoader.READER);
            
            FASTReaderDispatchBase reader = (FASTReaderDispatchBase)dispatchClass.newInstance();
            
            
            
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }
    
    
}
