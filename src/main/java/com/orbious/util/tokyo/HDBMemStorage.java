package com.orbious.util.tokyo;

import java.io.File;

import com.orbious.util.Bytes;
import com.orbious.util.config.Config;
import com.orbious.util.config.ConfigException;

import tokyocabinet.HDB;

public class HDBMemStorage<K, V> extends MemStorage<K, V> implements IFileStorage {

  public HDBMemStorage(File filestore, Class<K> keytype, Class<V> valuetype) {
    super(filestore, HDB.class, keytype, valuetype);
  }

  /*
   * Interface methods.
   */

  public void writecfg() throws StorageException {
    String xmlstr;

    xmlstr = null;
    try {
      xmlstr = Config.xmlstr();
    } catch ( ConfigException ce ) {
      throw new StorageException("Failed to extract xml config", ce);
    }

    dbm.put(Bytes.serialize(Config.config_hdb_key), Bytes.serialize(xmlstr));
  }

  @Override
  public void close() throws StorageException {
    if ( dbm == null ) {
      System.out.println("DBM is null in CLOSE");
    }
    writecfg();
    super.close();
  }

  public String cfgstr() {
    byte[] value;

    value = dbm.get(Bytes.serialize(Config.config_hdb_key));
    return (String)Bytes.deserialize(value);
  }
}
