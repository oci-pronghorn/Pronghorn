package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringBuilderWriter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

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
                .decimal("d", 2, o->o.d1)
                .string("s", o -> o.s1)
                .array("a", (o,i,n) -> i < o.a1.length? o : null)
                    .integer((o, i, n, visit) -> visit.visit(o.a1[i]))
                .nullableArray("a2", o -> o.a2 == null,(o,i,n) -> i < o.a2.length? o : null)
                    .constantNull()
                .beginObject("nm", o -> o.nm)
                    .nullableBool("b", o->true, o->o.b2)
                    .nullableInteger("i", o->true, o->o.i2)
                    .nullableDecimal("d", 2, o->true, o->0)
                    .nullableString("s", o -> o.s2)
                .endObject()
                .beginObject("m", o->o.m)
                    .nullableBool("b", o->true, o->o.b2)
                    .nullableInteger("i", o->true, o->o.i2)
                    .nullableDecimal("d", 2, o->true, o->o.d2)
                    .nullableString("s", o -> o.s2)
                .endObject()
            .endObject();
        assertTrue(json.isLocked());

        json.render(out, new Bucket());
        //System.out.println(out);
    }
}