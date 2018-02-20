package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringBuilderWriter;
import com.ociweb.pronghorn.pipe.PipeWriter;
import org.junit.Before;
import org.junit.Test;

class Bucket {
    boolean b1;
    int i1;
    double d1 = 123.4;
    String s1 = "bob";
    int[] a1 = new int[] { 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
    int[] a2 = null;

    Pale nm = null;
    Pale m = new Pale();
}

class Pale {
    boolean b2;
    int i2;
    double d2;
    String s2 = null;
}

public class JSONBucketTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testJson() {

        final JSONRenderer<Bucket> json = new JSONRenderer<Bucket>(new JSONKeywordsPretty())
            .beginObject()
                .bool("b", o -> o.b1)
                .integer("i", o -> o.i1)
                .decimal("d", (o, v) -> v.visit(o.d1, 2))
                .string("s", o -> o.s1)
                .array("a", (o,i,n) -> i < o.a1.length? o : null)
                    .integer((o, i, n, visit) -> visit.visit(o.a1[i]))
                .nullableArray("a2", o -> o.a2 == null,(o,i,n) -> i < o.a2.length? o : null)
                    .constantNull()
                .beginNullableObject("nm", o -> o.nm == null)
                    .nullableBool("b", (o, v) -> v.visit(o.nm.b2, true))
                    .nullableInteger("i", (o, v) -> v.visit(o.nm.i2, true))
                    .nullableDecimal("d", (o, v) -> v.visit(0, 0, true))
                    .nullableString("s", o -> o.nm.s2)
                .endObject()
                .beginObject("m")
                    .nullableBool("b", (o, v) -> v.visit(o.m.b2, true))
                    .nullableInteger("i", (o, v) -> v.visit(o.m.i2, true))
                    .nullableDecimal("d", (o, v) -> v.visit(o.m.d2, 2, true))
                    .nullableString("s", o -> o.m.s2)
                .endObject()
            .endObject();

        json.render(out, new Bucket());
        System.out.println(out);
    }
}