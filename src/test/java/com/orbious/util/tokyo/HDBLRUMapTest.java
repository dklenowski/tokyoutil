package com.orbious.util.tokyo;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import java.io.File;
import org.junit.Test;
import com.orbious.util.Loggers;
import tokyocabinet.HDB;

public class HDBLRUMapTest {

  public HDBLRUMapTest() {
    Loggers.init();
  }

  private void createTmpTokyoFile(File f) {
    HDB hdb;
    hdb = new HDB();
    hdb.open(f.toString(), HDB.OWRITER | HDB.OCREAT);

    for ( int i = 0; i < 1000; i++ ) {
      hdb.put(Bytes.strToBytes(Integer.toString(i)), Bytes.convert(Integer.class, i));
    }
    hdb.put(Bytes.intToBytes(10), Bytes.intToBytes(100));
    hdb.close();
  }

  @Test
  public void type() throws Exception {
    assertThat(HDBLRUMap.class, notNullValue());
  }

  @Test
  public void writer() throws Exception {
    File f;
    HDBLRUMap<String, Integer> map;

    f = File.createTempFile("HDBLRUMapTest", ".hdb");
    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, false);
    map.load();

    for ( int i = 0; i < 100; i++ ) {
      map.put(Integer.toString(i), i);
    }
    map.close();

    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, true);
    map.load();

    for ( int i = 0; i < 100; i++ ) {
      assertThat(map.get(Integer.toString(i)), notNullValue());
    }
    map.close();
  }

  @Test
  public void reader() throws Exception {
    File f;
    HDBLRUMap<String, Integer> map;

    f = File.createTempFile("HDBLRUMapTest", ".hdb");
    createTmpTokyoFile(f);

    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, true);
    map.load();

    for ( int i = 0; i < 100; i++ ) {
      assertThat(map.get(Integer.toString(i)), notNullValue());
    }
    map.close();
  }
}
