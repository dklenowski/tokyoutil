package com.orbious.util.tokyo;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.log4j.Logger;
import com.orbious.util.Bytes;
import com.orbious.util.Loggers;
import com.orbious.util.config.Config;

import tokyocabinet.DBM;

public abstract class MemStorage<K, V> implements IStorage {

  protected final File filestore;
  protected final Class<?> filetype;
  protected final Class<K> keytype;
  protected final Class<V> valuetype;
  protected HashMap<K, V> map;
  protected DBM dbm;
  protected Logger logger;

  public MemStorage(File filestore, Class<?> filetype, Class<K> keytype,
      Class<V> valuetype) {
    this.filestore = filestore;
    this.filetype = filetype;
    this.keytype = keytype;
    this.valuetype = valuetype;
    this.map = new HashMap<K,V>();
    logger = Loggers.logger();
  }

  public MemStorage(String filename, Class<?> filetype, Class<K> keytype,
      Class<V> valuetype) {
    this(new File(filename), filetype, keytype, valuetype);
  }

  public boolean exists() {
    return true;
  }

  public boolean isopen() {
    if ( dbm != null ) {
      return true;
    }

    return false;
  }

  public boolean readOnly() {
    return false;
  }

  public String path() {
    return filestore.toString();
  }

  public void open() throws StorageException {
    byte[] key;
    byte[] cfgkey;

    try {
      dbm = Helper.open(filestore, filetype, -1, false);
    } catch ( HelperException he ) {
      throw new StorageException("Failed to open " + filestore.toString(), he);
    }

    cfgkey = Bytes.serialize(Config.config_hdb_key);

    dbm.iterinit();
    while ( (key = dbm.iternext()) != null ) {
      if ( Arrays.equals(key, cfgkey) ) {
        continue;
      }
      map.put(keytype.cast(Bytes.deserialize(key)),
          valuetype.cast(Bytes.deserialize(dbm.get(key))));
    }
  }

  public long size() {
    return map.size();
  }

  public void clear() {
    map.clear();
  }

  public void close() throws StorageException {
    Iterator<K> iter;
    K key;

    iter = map.keySet().iterator();
    while ( iter.hasNext() ) {
      key = iter.next();
      dbm.put(Bytes.serialize(key), Bytes.serialize(map.get(key)));
    }

    try {
      Helper.close(dbm);
      dbm = null;
    } catch ( HelperException he ) {
      throw new StorageException("Error closing " + filestore.toString(), he);
    }
  }

  public V get(K key) {
    return map.get(key);
  }

  public V put(K key, V value) {
    return map.put(key, value);
  }

}
