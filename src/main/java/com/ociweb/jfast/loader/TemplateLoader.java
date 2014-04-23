//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;

public class TemplateLoader {

    private static final int BUILDING_EXCEPTION = -6;
    private static final int FILE_NOT_FOUND = -5;
    private static final int FILE_REQUIRED_NOT_DIRECTORY = -4;
    private static final int NO_WRITE_RIGHTS = -3;
    private static final int MISSING_REQ_ARG = -2;
    private static final int MISSING_ARG_VALUE = -1;

    /**
     * Load templates into catalog file.
     * 
     * The jFAST engine will support dynamic template changes among those in the
     * catalog. Building the catalog also validates no conflicts exist between
     * the contained templates. The catalog file can be replaced while jFAST is
     * running however it will not be accessed unless an unknown template id is
     * encountered or it is explicitly requested.
     * 
     * -s -source <folder or XML file as exclusive input into new catalog> -c
     * -catalog <absolute path to catalog file>
     * 
     * @param args
     */

    public static void main(String[] args) {

        File catalog = new File(getReqArg("-catalog", "-c", args));
        File source = new File(getReqArg("-source", "-s", args));

        if (!catalog.canWrite()) {
            printHelp("Unable to write to location: " + catalog);
            System.exit(NO_WRITE_RIGHTS);
        }
        if (catalog.isDirectory()) {
            printHelp("Catalog must be a file not a directory: " + catalog);
            System.exit(FILE_REQUIRED_NOT_DIRECTORY);
        }
        if (!source.exists()) {
            printHelp("Unable to find source: " + source);
            System.exit(FILE_NOT_FOUND);
        }

        try {
            buildCatalog(new FileOutputStream(catalog), source);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(BUILDING_EXCEPTION);
        }
    }

    public static void buildCatalog(OutputStream catalog, File source) throws ParserConfigurationException,
            SAXException, IOException {
        SAXParserFactory spfac = SAXParserFactory.newInstance();

        // //need to load fast.schema file
        // Schema schema = new Schema();
        // spfac.setSchema(schema);

        SAXParser sp = spfac.newSAXParser();
        FASTOutput output = new FASTOutputStream(catalog);
        TemplateHandler handler = new TemplateHandler(output);
        System.err.println(source);
        if (source.isFile()) {
            sp.parse(source, handler);
        } else {
            for (File f : source.listFiles()) {
                if (f.isFile()) {
                    sp.parse(f, handler);
                }
            }
        }

        handler.postProcessing();
    }

    private static void printHelp(String message) {
        System.out.println(message);
        System.out.println();
        System.out.println("Usage:");
        System.out.println("       TemplateLoader -s <templates folder or template file> -c <catalog file>");
        System.out.println();
        System.out.println("Arguments:");
        System.out.println("          -s or -source       full path to xml template or folder of templates.");
        System.out.println("          -c or -catalog      full path to catalog file to write over.");
        System.out.println();
    }

    private static String getReqArg(String longName, String shortName, String[] args) {
        String prev = null;
        for (String token : args) {
            if (longName.equals(prev) || shortName.equals(prev)) {
                if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
                    printHelp("Expected value not found");
                    System.exit(MISSING_ARG_VALUE);
                }
                return token.trim();
            }
            prev = token;
        }
        printHelp("Expected value not found");
        System.exit(MISSING_REQ_ARG);
        return null;
    }
}
