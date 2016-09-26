package com.ociweb.pronghorn.stage.network;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class TestDataFiles {
    public Path[] testFilePaths;
    public byte[][] testFilePathsBytes;
    public File tempDirectory;
    public int rootLen;

    public TestDataFiles(File tempDirectory, int fileCount, int fileSize) {
        this.tempDirectory = tempDirectory;
        buildTestFiles(fileCount, fileSize);
        rootLen = tempDirectory.toString().endsWith("/")|tempDirectory.toString().endsWith("\\") ? tempDirectory.toString().length()+1 : tempDirectory.toString().length();
    }
    
    private void buildTestFiles(int fileCount, int fileSize) {
        try {
            int c = fileCount;
            Random rand = new Random(42);
            testFilePaths = new Path[c];
            testFilePathsBytes = new byte[c][];
            
            tempDirectory.mkdir();            
            
            while (--c>=0) {
                    //build dummy files
                    File f = File.createTempFile("fake"+c,".test", tempDirectory);
                    f.deleteOnExit();
                    
                    assert (f.getAbsolutePath().startsWith(System.getProperty("java.io.tmpdir")));
                    
                    ByteBuffer fakeData = ByteBuffer.allocate(fileSize);
                    int j = fileSize;
                    while (--j >=0 ) {
                        fakeData.put((byte)rand.nextInt(255));   
                    }
                    fakeData.flip();
                    
                    Path path = f.toPath();
                    FileChannel channel = FileChannel.open(path,StandardOpenOption.WRITE, StandardOpenOption.SYNC);                
                    do {
                        channel.write(fakeData);
                    } while (fakeData.hasRemaining());
                    channel.close();
                    
                    testFilePaths[c] = path;
                    testFilePathsBytes[c] = path.toString().substring(rootLen).getBytes();
                
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}