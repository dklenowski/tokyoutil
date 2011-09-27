package com.orbious.util.tokyo;

import java.io.File;
import java.io.IOException;
import tokyocabinet.HDB;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Test;
import com.orbious.util.Bytes;

// TODO: More tests

public class HDBStorageTest {

  private void createTmpTokyoFile(File f) {
    HDB hdb;
    hdb = new HDB();
    hdb.open(f.toString(), HDB.OWRITER | HDB.OCREAT);
    hdb.put(Bytes.intToBytes(10), Bytes.intToBytes(100));
    hdb.close();
  }

	@Test
	public void type() throws Exception {
		assertThat(HDBStorage.class, notNullValue());
	}

  @Test
  public void initReader() throws IOException, StorageException {
    File f;
    HDBStorage hdbs;

    f = File.createTempFile("WrapperTest", ".hdbs");
    createTmpTokyoFile(f);

    hdbs = new HDBStorage(f, 10, true);
    hdbs.open();

    assertThat(hdbs, notNullValue());
    assertThat(hdbs.dbm, notNullValue());

    hdbs.close();
    assertThat(hdbs.dbm, nullValue());
  }

  @Test(expected=StorageException.class)
  public void cannotWriteToReader() throws IOException, StorageException {
    File f;
    HDBStorage hdbs;

    f = File.createTempFile("WrapperTest", ".hdbs");

    hdbs = new HDBStorage(f, 10, true);
    hdbs.open();

    assertThat(hdbs, notNullValue());
    assertThat(hdbs.dbm, notNullValue());

    // this should throw an exception ..
    hdbs.write(1, "a string");
  }

  @Test
  public void initWriter() throws IOException, StorageException {
    File f;
    HDBStorage hdbs;

    f = File.createTempFile("WrapperTest", ".hdbs");

    hdbs = new HDBStorage(f, 1, false);
    hdbs.open();

    assertThat(hdbs, notNullValue());
    assertThat(hdbs.dbm, notNullValue());

    hdbs.close();
    assertThat(hdbs.dbm, nullValue());
  }

  @Test
  public void canWriteToWriter() throws IOException, StorageException {
    File f;
    HDBStorage hdbs;
    String actual;

    f = File.createTempFile("WrapperTest", ".hdbs");

    hdbs = new HDBStorage(f, 1, false);
    hdbs.open();

    assertThat(hdbs, notNullValue());
    assertThat(hdbs.dbm, notNullValue());

    hdbs.write(10, "a string");
    hdbs.write(2, "another string");

    actual = (String)hdbs.readObject(10);
    assertThat(actual, is(equalTo("a string")));
  }

  @Test
  public void intkeys() throws IOException, StorageException {
    File f;
    HDBStorage hdbs;

    f = File.createTempFile("WrapperTest", ".hdbs");

    hdbs = new HDBStorage(f, 1, false);
    hdbs.open();

    assertThat(hdbs, notNullValue());
    assertThat(hdbs.dbm, notNullValue());

    hdbs.write(10, "string value");
    hdbs.write(20, 20L);
    hdbs.write(25, 15D);

    assertThat((String)hdbs.readObject(10), is(equalTo("string value")));
    assertThat(hdbs.readLong(20), is(equalTo(20L)));
    assertThat(hdbs.readDouble(25), is(equalTo(15D)));

    hdbs.close();


    hdbs = new HDBStorage(f, 1, true);
    hdbs.open();

    assertThat((String)hdbs.readObject(10), is(equalTo("string value")));
    assertThat(hdbs.readLong(20), is(equalTo(20L)));
    assertThat(hdbs.readDouble(25), is(equalTo(15D)));

    hdbs.close();
  }
}
