package com.orbious.util.tokyo;

import com.orbious.util.Bytes;
import com.orbious.util.tokyo.FDBFile;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Test;
import java.io.File;

public class FDBFileTest {

	@Test
	public void type() throws Exception {
		assertThat(FDBFile.class, notNullValue());
	}

	@Test
	public void instantiation() throws Exception {
		File filestore = new File("/tmp/test.fdb");
		boolean readOnly = false;
		FDBFile target = new FDBFile(filestore, readOnly);
		assertThat(target, notNullValue());
	}

	@Test
	public void objs() throws Exception {
	  File file;
	  FDBFile fdbf;
	  String expected, expected2;
	  int i;

	  file = File.createTempFile("FDBFileTest", ".fdbs");
	  fdbf = new FDBFile(file, false);
	  fdbf.open();

	  expected = "A test string";
	  i = fdbf.put(expected);
	  assertThat(i, is(equalTo(2)));

	  expected2 = "Another test string";
	  i = fdbf.put(expected2);
	  assertThat(i, is(equalTo(3)));

	  assertThat((String)fdbf.getObject(2), is(equalTo(expected)));
    assertThat((String)fdbf.getObject(3), is(equalTo(expected2)));

	  fdbf.close();

	  fdbf = new FDBFile(file, true);
	  fdbf.open();
	  assertThat((String)fdbf.getObject(2), is(equalTo(expected)));
	  assertThat((String)fdbf.getObject(3), is(equalTo(expected2)));
	  assertThat(fdbf.index(), is(equalTo(4)));
	}

	 @Test
	 public void bytes() throws Exception {
	   File file;
	   FDBFile fdbf;
	   byte[] expected, expected2;
	   int i;

	   file = File.createTempFile("FDBFileTest", ".fdbs");
	   fdbf = new FDBFile(file, false);
	   fdbf.open();

	   expected = Bytes.strToBytes("A test string");
	   i = fdbf.put(expected);
	   assertThat(i, is(equalTo(2)));

	   expected2 = Bytes.strToBytes("Another test string");
	   i = fdbf.put(expected2);
	   assertThat(i, is(equalTo(3)));

	   assertThat(Bytes.bytesToStr(fdbf.get(2)), is(equalTo(Bytes.bytesToStr(expected))));
     assertThat(Bytes.bytesToStr(fdbf.get(3)), is(equalTo(Bytes.bytesToStr(expected2))));

	   fdbf.close();

	   fdbf = new FDBFile(file, true);
	   fdbf.open();
     assertThat(Bytes.bytesToStr(fdbf.get(2)), is(equalTo(Bytes.bytesToStr(expected))));
     assertThat(Bytes.bytesToStr(fdbf.get(3)), is(equalTo(Bytes.bytesToStr(expected2))));
	   assertThat(fdbf.index(), is(equalTo(4)));
	 }

	  @Test
	  public void ints() throws Exception {
	    File file;
	    FDBFile fdbf;
	    int expected, expected2;
	    int i;

	    file = File.createTempFile("FDBFileTest", ".fdbs");
	    fdbf = new FDBFile(file, false);
	    fdbf.open();

	    expected = Integer.MIN_VALUE;
	    i = fdbf.put(expected);
	    assertThat(i, is(equalTo(2)));

	    expected2 = Integer.MAX_VALUE;
	    i = fdbf.put(expected2);
	    assertThat(i, is(equalTo(3)));

	    assertThat(fdbf.getInt(2), is(equalTo(expected)));
	    assertThat(fdbf.getInt(3), is(equalTo(expected2)));

	    fdbf.close();

	    fdbf = new FDBFile(file, true);
	    fdbf.open();
	    assertThat(fdbf.getInt(2), is(equalTo(expected)));
	    assertThat(fdbf.getInt(3), is(equalTo(expected2)));
	    assertThat(fdbf.index(), is(equalTo(4)));
	  }

    @Test
    public void longs() throws Exception {
      File file;
      FDBFile fdbf;
      long expected, expected2;
      int i;

      file = File.createTempFile("FDBFileTest", ".fdbs");
      fdbf = new FDBFile(file, false);
      fdbf.open();

      expected = Long.MIN_VALUE;
      i = fdbf.put(expected);
      assertThat(i, is(equalTo(2)));

      expected2 = Long.MAX_VALUE;
      i = fdbf.put(expected2);
      assertThat(i, is(equalTo(3)));

      assertThat(fdbf.getLong(2), is(equalTo(expected)));
      assertThat(fdbf.getLong(3), is(equalTo(expected2)));

      fdbf.close();

      fdbf = new FDBFile(file, true);
      fdbf.open();
      assertThat(fdbf.getLong(2), is(equalTo(expected)));
      assertThat(fdbf.getLong(3), is(equalTo(expected2)));
      assertThat(fdbf.index(), is(equalTo(4)));
    }

    @Test
    public void doubles() throws Exception {
      File file;
      FDBFile fdbf;
      double expected, expected2;
      int i;

      file = File.createTempFile("FDBFileTest", ".fdbs");
      fdbf = new FDBFile(file, false);
      fdbf.open();

      expected = Double.MIN_VALUE;
      i = fdbf.put(expected);
      assertThat(i, is(equalTo(2)));

      expected2 = Double.MAX_VALUE;
      i = fdbf.put(expected2);
      assertThat(i, is(equalTo(3)));

      assertThat(fdbf.getDouble(2), is(equalTo(expected)));
      assertThat(fdbf.getDouble(3), is(equalTo(expected2)));

      fdbf.close();

      fdbf = new FDBFile(file, true);
      fdbf.open();
      assertThat(fdbf.getDouble(2), is(equalTo(expected)));
      assertThat(fdbf.getDouble(3), is(equalTo(expected2)));
      assertThat(fdbf.index(), is(equalTo(4)));
    }
}
