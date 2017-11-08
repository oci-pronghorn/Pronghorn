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

//import BinaryTree.BinaryTree;


public class HuffmanDecoder {

	//BinaryTree tree = new BinaryTree();

    public static void main(String[] args){
        int in;
        byte current = (byte)0xaa; //0xaa just because I needed something here. 
        byte last = (byte)0xaa;    //0xaa has no significance
        int[] data;

        try {
            // FileInputStream f = new FileInputStream("nathan.jpg");
            FileInputStream f = new FileInputStream("cat.jpg");
            
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

        //info about the huffman table in question
        println("Half bytes:");
        println(formatByte(firstHalf));
        println(formatByte(secondHalf));

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

    //fill byte array with all 0xFF to help visualize huffman elements. 
    //this will only be used temporarily, not in final release
    // public static byte[][] fill(byte[][] m){
    //     for(int i = 0; i < m.length; ++i){
    //         for(int j = 0; j < m[i].length; ++j){
    //             m[i][j] = (byte)0xFF;
    //         }
    //     }
    //     return m;
    // }

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



/*
public buildTree(byte[] numElements, ArrayList<byte>[] values) {

        for(int i = 0; i < values.length; ++i) {
            for(int j = 0; j < numElements[i]; ++j) {
                placeNode(0, values[i][j], j + 1);
                // 0 = root, values[i][j] = code, j + 1 = codeLength
            }
        }
    }

    //placing a node based on the jpg huffman tree rebuild algorithm
    //https://www.imperialviolet.org/binary/jpeg/
    public static void placeNode(int position, byte code, int codeLength) {
        int currentPosition = 0;

        for(int i = 0; i < codeLength; ++i){
            if(tree.left(currentPosition) != null && tree.left(currentPosition).leaf == false) {
                currentPosition = right(currentPosition);
            } else(tree.right(currentPosition) != null && tree.left(currentPosition).leaf == true) {
                currentPosition = left(currentPosition);
            }
        }
    }
    */