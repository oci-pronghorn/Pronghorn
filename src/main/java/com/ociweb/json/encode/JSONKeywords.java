package com.ociweb.json.encode;

// TODO: Once we develop the pretty output the exact methods may change.

import com.ociweb.json.appendable.ByteWriter;

public class JSONKeywords {
    private static final byte[] True = "true".getBytes();
    private static final byte[] False = "false".getBytes();
    private static final byte[] Null = "null".getBytes();
    private static final byte[] Quote = "\"".getBytes();

    private static final byte[] Start = "".getBytes();
    private static final byte[] Complete = "".getBytes();

    private static final byte[] OpenObj = "{".getBytes();
    private static final byte[] CloseObj = "}".getBytes();
    private static final byte[] OpenArray = "[".getBytes();
    private static final byte[] CloseArray = "]".getBytes();

    private static final byte[] FirstObjectElement = "\"".getBytes();
    private static final byte[] NextObjectElement = ",\"".getBytes();
    private static final byte[] ObjectValue = "\":".getBytes();
    private static final byte[] NextArrayElement = ",".getBytes();

    public void True(ByteWriter writer) {
        writer.write(True);
    }

    public void False(ByteWriter writer) {
        writer.write(False);
    }

    public void Null(ByteWriter writer) {
        writer.write(Null);
    }

    public void Quote(ByteWriter writer) {
        writer.write(Quote);
    }

    public void Start(ByteWriter writer, int depth) {
        writer.write(Start);
    }

    public void Complete(ByteWriter writer, int depth) {
        writer.write(Complete);
    }

    public void OpenObj(ByteWriter writer, int depth) {
        writer.write(OpenObj);
    }

    public void CloseObj(ByteWriter writer, int depth) {
        writer.write(CloseObj);
    }

    public void OpenArray(ByteWriter writer, int depth) {
        writer.write(OpenArray);
    }

    public void CloseArray(ByteWriter writer, int depth) {
        writer.write(CloseArray);
    }

    public void FirstObjectElement(ByteWriter writer, int depth) {
        writer.write(FirstObjectElement);
    }

    public void NextObjectElement(ByteWriter writer, int depth) {
        writer.write(NextObjectElement);
    }

    public void ObjectValue(ByteWriter writer, int depth) {
        writer.write(ObjectValue);
    }

    public void NextArrayElement(ByteWriter writer, int depth) {
        writer.write(NextArrayElement);
    }

}
