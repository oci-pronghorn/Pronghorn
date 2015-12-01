package com.ociweb.pronghorn.stage.test;

public class TestClassLoader extends ClassLoader{

    private final String name;
    private final byte[] classData;
    
    public TestClassLoader(String name, byte[] classData) {
        this.name = name;
        this.classData = classData;
    }
    
    @Override
    public Class loadClass(String name) throws ClassNotFoundException {
        //if we want a normal class use the normal class loader
        if(!name.equals(this.name)) {
            return super.loadClass(name);
        }

        //returning with defineClass helps reduce the risk that we may try to define the name again.
        return defineClass(name, classData , 0, classData.length);                 
    }

}
