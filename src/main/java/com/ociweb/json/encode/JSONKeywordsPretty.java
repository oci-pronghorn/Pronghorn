package com.ociweb.json.encode;

import com.ociweb.json.appendable.ByteWriter;

public class JSONKeywordsPretty extends JSONKeywords {
    private static final byte[] tabs =    "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t".getBytes();
    private static final int maxDepth = tabs.length;

    private static final byte[] OpenObj =    "{\n".getBytes();
    private static final byte[] ObjectValue = "\": ".getBytes();
    private static final byte[] NextObjectElement = ",\n\"".getBytes();
    private static final byte[] CloseObj =   "\n}".getBytes();
    private static final byte[] OpenArray =  "[\n".getBytes();
    private static final byte[] NextArrayElement = ",\n".getBytes();
    private static final byte[] CloseArray = "\n]".getBytes();
    private static final byte[] Complete =   "\n".getBytes();

    private void write(ByteWriter writer, byte[] bytes, int tabPlacement, int depth) {
        if (tabPlacement > 0) {
            writer.write(bytes, 0, tabPlacement);
        }
        if (depth > 0) {
            depth = Math.min(maxDepth, depth);
            writer.write(tabs, 0, depth);
        }
        if (tabPlacement < bytes.length) {
            writer.write(bytes, tabPlacement, bytes.length - tabPlacement);
        }
    }

    @Override
    public void Complete(ByteWriter writer, int depth) {
        write(writer, Complete, 0, depth);
    }

    @Override
    public void OpenObj(ByteWriter writer, int depth) {
        write(writer, OpenObj, 2, depth+ 1);
    }

    @Override
    public void CloseObj(ByteWriter writer, int depth) {
        write(writer, CloseObj, 1, depth-1);
    }

    @Override
    public void OpenArray(ByteWriter writer, int depth) {
        write(writer, OpenArray, 2, depth + 1);
    }

    @Override
    public void CloseArray(ByteWriter writer, int depth) {
        write(writer, CloseArray, 1, depth-1);
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
        write(writer, NextArrayElement, 2, depth);
    }
}
