//for some reason adding to the package is breaking this. 
// package com.ociweb.jpgRaster; 


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
// import apache.*;


public class HuffmanDecoder {

    public static void main(String[] args){
        int in;
        byte current = (byte)0xaa; //0xaa just because I needed something here. 
        byte last = (byte)0xaa;    //0xaa has no significance
        byte[] data;

        try {
            // FileInputStream f = new FileInputStream("nathan.jpg");
            FileInputStream f = new FileInputStream("cat.jpg");
            
            while((in = f.read()) != -1){

                current = (byte)in;
                
                if(last == (byte)0xFF && current == (byte)0xc4){
                    
                    data = grabData(f);

                    interpretHuffmanSegment(data);
                    //handle huffman stuff here
                    break;
                }
                last = current;
                println(formatByte(last));
                System.out.println(Integer.toBinaryString((in & 0xFF) + 0x100).substring(1));
            }
        } catch(IOException e) {

        }
        System.out.println("done");
    }

    public static void interpretHuffmanSegment(byte[] b) throws IOException {
        //I will use an input stream to keep my place in the data. 
        ByteArrayInputStream bytes = new ByteArrayInputStream(b);
        ArrayList<Byte>[] values1 = interpretDHT(bytes);
        ArrayList<Byte>[] values2 = interpretDHT(bytes);
        ArrayList<Byte>[] values3 = interpretDHT(bytes);
        ArrayList<Byte>[] values4 = interpretDHT(bytes);
    }

    public static ArrayList<Byte>[] interpretDHT(ByteArrayInputStream bytes){
        byte firstByte = 0;
        byte firstHalf = 0;
        byte secondHalf = 0;
        byte[] numElements = new byte[16];
        ArrayList<Byte>[] values = (ArrayList<Byte>[])new ArrayList[16];
        for(int i = 0; i < 16; ++i) { values[i] = new ArrayList<Byte>(); }
        ArrayList<Short>[] codes;


        //First byte (in halves) should be HT information for each table
        firstByte = (byte)bytes.read();
        firstHalf = (byte)(firstByte & 0xF0);
        secondHalf = (byte)(firstByte & 0x0F);

        println("numbers of elements:");
        for(int i = 0; i < 16; ++i){
            numElements[i] = (byte)bytes.read();
            println(formatByte(numElements[i]) + " " + (i + 1)); //print byte as hex
        }

        print("\n\n\n");
        for(int i = 0; i < 16; ++i) {
            for(int j = 0; j < numElements[i]; ++j){
                values[i].add((byte)bytes.read());
            }
        }
        printHuffValues(values);
        print("\n\nCodes:\n");

        codes = generateCodes(numElements);
        printHuffCodes(codes);
        println("+++++++++++++++++++++++++++++++++++++++++++\n\n\n");
        return values;
    }

    public static ArrayList<Short>[] generateCodes(byte[] sizes){
        ArrayList<Short>[] codes = (ArrayList<Short>[])new ArrayList[16];
        for(int i = 0; i < 16; ++i) { codes[i] = new ArrayList<Short>(); }

        short code = 0;
        for(int i = 0; i < sizes.length; ++i){
            for(int j = 0; j < sizes[i]; ++j){
                codes[i].add(code);
                ++code;
            }
            code <<= 1;
        }
        return codes;
    }

    



    public static void printHuffCodes(ArrayList<Short>[] c){
        for(int i = 0; i < c.length; ++i){
            print((i + 1) + " ");
            for(int j = 0; j < c[i].size(); ++j){
                print(String.format("%04x", c[i].get(j)) + " ");
            }
            print("\n");
        }
    }

    //simply prints a 2d array of bytes
    public static void printHuffValues(ArrayList<Byte>[] m){
        for(int i = 0; i < m.length; ++i){
            print((i + 1) + "   ");
            for(int j = 0; j < m[i].size(); ++j){
                print(formatByte(m[i].get(j)) + ":");
                // print(Integer.toBinaryString((m[i].get(j) & 0xFF) + 0x100).substring(1) + "  ");
            }
            System.out.println();
        }
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

    public static byte[] grabData(FileInputStream f) throws IOException{
        short length;
        byte[] data;

        //The number denoting the length of data is two bytes long and follows the data indicator. 
        length = (short)(((short)f.read() << 8) | ((short)f.read() & 0xFF)); //basically concatenation of two binary strings. 
        data = new byte[length];
        for(int i = 0; i < length - 2; ++i){
            data[i] = (byte)f.read();
        }

        return data;
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