package com.orbious.tokyo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import tokyocabinet.HDB;
import tokyocabinet.Util;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Test;

// TODO: More tests

public class HDBWrapperTest {

  private void createTmpTokyoFile(File f) {
    HDB hdb;
    hdb = new HDB();
    hdb.open(f.toString(), HDB.OWRITER | HDB.OCREAT);
    hdb.put(ByteUtils.intToBytes(10), ByteUtils.intToBytes(100));
    hdb.close();
  }

	@Test
	public void type() throws Exception {
		assertThat(HDBWrapper.class, notNullValue());
	}

  @Test
  public void initReader() throws IOException, WrapperException {
    File f;
    HDBWrapper hdbw;

    f = File.createTempFile("WrapperTest", ".hdb");
    createTmpTokyoFile(f);

    hdbw = new HDBWrapper();
    hdbw.initReader(f, 10);
    assertThat(hdbw, notNullValue());
    assertThat(hdbw.hdb, notNullValue());

    hdbw.close();
    assertThat(hdbw.hdb, nullValue());
  }

  @Test(expected=WrapperException.class)
  public void cannotWriteToReader() throws IOException, WrapperException {
    File f;
    HDBWrapper hdbw;

    f = File.createTempFile("WrapperTest", ".hdb");

    hdbw = new HDBWrapper();
    hdbw.initReader(f, 10);
    assertThat(hdbw, notNullValue());
    assertThat(hdbw.hdb, notNullValue());

    // this should throw an exception ..
    hdbw.write(1, "a string");
  }

  @Test
  public void initWriter() throws IOException, WrapperException {
    File f;
    HDBWrapper hdbw;

    f = File.createTempFile("WrapperTest", ".hdb");

    hdbw = new HDBWrapper();
    hdbw.initWriter(f, 1);

    assertThat(hdbw, notNullValue());
    assertThat(hdbw.hdb, notNullValue());

    hdbw.close();
    assertThat(hdbw.hdb, nullValue());
  }

  @Test
  public void canWriteToWriter() throws IOException, WrapperException {
    File f;
    HDBWrapper hdbw;
    String actual;

    f = File.createTempFile("WrapperTest", ".hdb");

    hdbw = new HDBWrapper();
    hdbw.initWriter(f, 1);

    assertThat(hdbw, notNullValue());
    assertThat(hdbw.hdb, notNullValue());

    hdbw.write(10, "a string");
    hdbw.write(2, "another string");

    actual = (String)hdbw.readObject(10);
    assertThat(actual, is(equalTo("a string")));
  }





}
