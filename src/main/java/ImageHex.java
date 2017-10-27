import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ImageHex {

    public static void main(String[] args){
    	int in;
    	byte current = (byte)0xaa; //0xaa just because I needed something here. 
    	byte last = (byte)0xaa;    //0xaa has no significance
        int[] data;

        try {
            FileInputStream f = new FileInputStream("huff_simple0.jpg");
            
            while((in = f.read()) != -1){

            	current = (byte)in;
            	
            	if(last == (byte)0xFF && current == (byte)0xc4){
            		println(String.format("%02x", current));
            		println("Huffman Indicator found\n");
                    
            		data = grabData(f);
                    System.out.println("The huffman data length is: " + data.length);
                    printBytes(data);
                    //handle huffman stuff here
            		break;
            	}
            	last = current;
            	println(String.format("%02x", last));
                System.out.println(Integer.toBinaryString((in & 0xFF) + 0x100).substring(1));
            }
        } catch(IOException e) {

        }
        System.out.println();
    }

    public static int[] grabData(FileInputStream f) throws IOException{
        int length;
        int[] data;

        //The number denoting the length of data is two bytes long and follows the data indicator. 
        length = ((f.read() << 8) | (f.read() & 0xFF)); //basically concatenation of two binary strings. 
        data = new int[length];
        for(int i = 0; i < length; ++i){
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
            print(String.format("%02x", bytes[i]) + " ");
        }
    }

    public static void print(Object o){
        System.out.print(o);
    }
    public static void println(Object o){
        System.out.println(o);
    }
}