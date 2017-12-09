package jpgRaster;

import jpgRaster.HuffmanDecoder;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

public class HuffmanDriver {

    static HuffmanDecoder decoder = new HuffmanDecoder();
    public static void main(String[] args){
        int in;
        byte current = (byte)0xaa; //0xaa just because I needed something here. 
        byte last = (byte)0xaa;    //0xaa has no significance
        byte[] data;
        

        try {
            // FileInputStream f = new FileInputStream("jpgRaster/nathan.jpg");
            FileInputStream f = new FileInputStream("jpgRaster/cat.jpg");
            
            while((in = f.read()) != -1){

                current = (byte)in;
                
                if(last == (byte)0xFF && current == (byte)0xc4){
                    
                    data = grabData(f);

                    decoder.interpretHuffmanSegment(data);
                    //handle huffman stuff here with decoder messages
                    break;
                }
                last = current;
            }
        } catch(IOException e) {

        }
        System.out.println("done");
    }
    
    public static byte[] grabData(FileInputStream f) throws IOException{
        short length;
        byte[] data;

        //The number denoting the length of data is two bytes long and follows the data indicator. 
        length = (short)(((short)f.read() << 8) | ((short)f.read() & 0xFF)); //basically concatenation of two binary strings. 
        data = new byte[length];
        for(int i = 0; i < length - 2; ++i){
            data[i] = (byte)f.read();
            if(i % 4 == 0 && i != 0) System.out.print("  ");
            if(i % 8 == 0 && i != 0) System.out.print("\n");
            System.out.print(formatByte(data[i]));
        }

        return data;
    }

    public static String formatByte(byte b){
        return String.format("%02x", b);
    }
}