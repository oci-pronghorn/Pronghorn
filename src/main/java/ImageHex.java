/*

The values I'm currently getting don't seem to make sense. 
Essentially, I'm seeing that huff_simple0.jpg's first huffman table
should have 7 elements of length 3. Then I'm seeing repetition
in the elements, such as multiple instances of 000 or 100. 

I think I may just be a little mixed up about how these are actually
stored, I'm seeing conflicting (but similar) descriptions when I 
look at different sources of information. 

*/


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import apache.*;


public class ImageHex {

    public static void main(String[] args){
        int in;
        byte current = (byte)0xaa; //0xaa just because I needed something here. 
        byte last = (byte)0xaa;    //0xaa has no significance
        int[] data;

        try {
            // FileInputStream f = new FileInputStream("nathan.jpg");
            FileInputStream f = new FileInputStream("huff_simple0.jpg");
            
            while((in = f.read()) != -1){

                current = (byte)in;
                
                if(last == (byte)0xFF && current == (byte)0xc4){
                    println(formatByte(current));
                    println("Huffman Indicator found\n");
                    
                    data = grabData(f);
                    
                    println("The huffman data length is: " + data.length);
                    
                    printBytes(data);

                    System.out.println();
                    println("That is the data. Interpreting now");

                    interpretHuffmanSegment(convertIntArrayToByteArray(data));
                    //handle huffman stuff here
                    break;
                }
                last = current;
                println(formatByte(last));
                System.out.println(Integer.toBinaryString((in & 0xFF) + 0x100).substring(1));
            }
        } catch(IOException e) {

        }
        System.out.println();
    }


    public static void interpretHuffmanSegment(byte[] b) throws IOException {
        //I will use an input stream to keep my place in the data. 
        ByteArrayInputStream bytes = new ByteArrayInputStream(b); //I'd like to skip this step
        BitStream bits = new BitStream(bytes);
        byte firstByte = 0;
        byte firstHalf = 0;
        byte secondHalf = 0;
        byte[] numElements = new byte[16];
        byte[][] values = new byte[16][16];

        values = fill(values); //filling with all 0xFF


        //First byte (in halves) should be HT information for table 1
        firstByte = (byte)bits.nextByte();
        firstHalf = (byte)(firstByte & 0xF0);
        secondHalf = (byte)(firstByte & 0x0F);

        //for the first table we expect these both to be 0
        println("Half bytes:");
        println(formatByte(firstHalf));
        println(formatByte(secondHalf));

        println("numbers of elements:");
        for(int i = 0; i < 16; ++i){
            numElements[i] = (byte)bits.nextByte();
            println(formatByte(numElements[i]) + " " + (i + 1)); //print byte as hex

            // println((byte)(numElements[i] * (i + 1)));
        }

        print("\n\n\n");
        //Values have been pre-filled with all 0xFF
        for(int i = 0; i < 16; ++i) {
            for(int j = 0; j < numElements[i]; ++j){
                values[i][j] = (byte)bits.nextBits(i + 1);
            }
        }
        printByteMatrix(values);
    }

    //simply prints a 2d array of bytes
    public static void printByteMatrix(byte[][] m){
        for(int i = 0; i < m.length; ++i){
            print((i + 1) + "   ");
            for(int j = 0; j < m[i].length; ++j){
                if(m[i][j] != (byte)0xFF) //Just using this to more easily visualize, I filled the array with all 0xFF before I read data
                    print(formatByte(m[i][j]) + ":");

                // print(Integer.toBinaryString((m[i][j] & 0xFF) + 0x100).substring(1) + "  ");
            }
            System.out.println();
        }
    }

    //fill byte array with all 0xFF to help visualize huffman elements. 
    //this will only be used temporarily, not in final release
    public static byte[][] fill(byte[][] m){
        for(int i = 0; i < m.length; ++i){
            for(int j = 0; j < m[i].length; ++j){
                m[i][j] = (byte)0xFF;
            }
        }
        return m;
    }

    public static int[] grabData(FileInputStream f) throws IOException{
        int length;
        int[] data;

        //The number denoting the length of data is two bytes long and follows the data indicator. 
        length = ((f.read() << 8) | (f.read() & 0xFF)); //basically concatenation of two binary strings. 
        data = new int[length];
        for(int i = 0; i < length - 2; ++i){
            data[i] = f.read();
        }

        return data;        
    }

    //print an array of bytes as hex
    public static void printBytes(int[] bytes) {
        for(int i = 0; i < bytes.length; ++i) {
            if (i % 4 == 0 && i != 0) System.out.print("  ");
            if (i % 8 == 0 && i != 0) System.out.print("\n");
            // System.out.println(String.format("%02x", bytes[i]) + " " + Integer.toBinaryString((bytes[i] & 0xFF) + 0x100).substring(1) + "   ");
            print(formatByte((byte)bytes[i]) + " ");
        }
    }

    //Interesting to note this is one line in python. return [(byte)x for x in bytes].
    public static byte[] convertIntArrayToByteArray(int[] bytes) {
        byte[] b = new byte[bytes.length];
        for(int i = 0; i < bytes.length; ++i){ b[i] = (byte)bytes[i]; }
        return b;
    }

    public static String formatByte(byte b){
        return String.format("%02x", b);
    }
    public static void print(Object o){
        System.out.print(o);
    }
    public static void println(Object o){
        System.out.println(o);
    }
}