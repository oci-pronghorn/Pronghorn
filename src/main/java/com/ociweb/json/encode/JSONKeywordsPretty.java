package com.ociweb.json.encode;

import com.ociweb.json.appendable.ByteWriter;

public class JSONKeywordsPretty extends JSONKeywords {
    public static final byte[] tabs =    "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t".getBytes();
    public static final int maxDepth = tabs.length;

    public static final byte[] NewLine =    "\n".getBytes();
    public static final byte[] OpenObj =    "\n{".getBytes();
    public static final byte[] CloseObj =   "}\n".getBytes();
    public static final byte[] OpenArray =  "[\n".getBytes();
    public static final byte[] CloseArray = "\n]".getBytes();

    private static final byte[] NextObjectElement = ",\n\"".getBytes();
    private static final byte[] ObjectValue = "\" : ".getBytes();
    private static final byte[] NextArrayElement = ",\n".getBytes();

    private void write(ByteWriter writer, byte[] bytes, int tabPlacement, int depth) {
        if (tabPlacement > 0) {
            writer.write(bytes, 0, tabPlacement);
        }
        if (depth > 0) {
            depth = Math.min(maxDepth, depth);
            writer.write(tabs, 0, depth);
        }
        if (tabPlacement < bytes.length) {
            writer.write(tabs, tabPlacement, bytes.length - tabPlacement);
        }
    }

    @Override
    public void Start(ByteWriter writer, int depth) {
        write(writer, NewLine, 0, depth);
    }

    @Override
    public void Complete(ByteWriter writer, int depth) {
        write(writer, NewLine, 0, depth);
    }

    @Override
    public void OpenObj(ByteWriter writer, int depth) {
        write(writer, OpenObj, 0, depth);
    }

    @Override
    public void CloseObj(ByteWriter writer, int depth) {
        write(writer, CloseObj, 1, depth);
    }

    @Override
    public void OpenArray(ByteWriter writer, int depth) {
        write(writer, OpenArray, 0, depth);
    }

    @Override
    public void CloseArray(ByteWriter writer, int depth) {
        write(writer, CloseArray, 1, depth);
    }

    @Override
    public void NextObjectElement(ByteWriter writer, int depth) {
        write(writer, NextObjectElement, 2, depth);
    }

    @Override
    public void ObjectValue(ByteWriter writer, int depth) {
        writer.write(ObjectValue);
    }

    @Override
    public void NextArrayElement(ByteWriter writer, int depth) {
        write(writer, NextArrayElement, 1, depth);
    }
}
