package com.orbious.util.tokyo;

import java.io.File;

import tokyocabinet.DBM;
import tokyocabinet.FDB;
import tokyocabinet.HDB;

public class Helper {

  private Helper() { }

  public static DBM open(File file, Class<?> clazz, int size, boolean readOnly)
      throws HelperException {
    DBM dbm;

    if ( clazz == HDB.class )
      dbm = openhdb(file, size, readOnly);
    else if ( clazz == FDB.class )
      dbm = openfdb(file, readOnly);
    else
      throw new UnsupportedOperationException("Unsupported storage type for tokyo storage");

    return dbm;
  }

  public static HDB openhdb(File file, int size, boolean readOnly)
      throws HelperException {
    HDB hdb = new HDB();
    if ( size != -1 ) hdb.setcache(size);

    int mode;
    if ( readOnly )
      mode = HDB.OREADER | HDB.ONOLCK;
    else
      mode = HDB.OWRITER | HDB.OCREAT;

    if ( !hdb.open(file.toString(), mode) ) {
      throw new HelperException("Failed to initialize hdb storage ",
          hdb.ecode(), HDB.errmsg(hdb.ecode()));
    }

    return hdb;
  }

  public static FDB openfdb(File file, boolean readOnly) throws HelperException {
    FDB fdb = new FDB();

    int mode;
    if ( readOnly )
      mode = FDB.OREADER | FDB.ONOLCK;
    else
      mode = FDB.OWRITER | FDB.OCREAT;

    if ( !fdb.open(file.toString(), mode) ) {
      throw new HelperException("Failed to initialize fdb storage ",
          fdb.ecode(), FDB.errmsg(fdb.ecode()));
    }

    return fdb;
  }

  public static void close(DBM dbm) throws HelperException {
    if ( dbm == null ) return;

    boolean doerror = false;
    if ( dbm instanceof HDB ) {
      if ( !((HDB)dbm).close() )
        doerror = true;
    } else if ( dbm instanceof FDB ) {
      if ( !((FDB)dbm).close() )
        doerror = true;
    } else {
      throw new UnsupportedOperationException("Unsupported storage type for tokyo storage");
    }

    if ( doerror )
      throw new HelperException("Failed to close tokyo storage ", ecode(dbm), errmsg(dbm));
  }

  public static int ecode(DBM dbm) {
    if ( dbm instanceof HDB )
      return ((HDB)dbm).ecode();
    else if ( dbm instanceof FDB )
      return ((FDB)dbm).ecode();

    return -1;
  }

  public static String errmsg(DBM dbm) {
    if ( dbm instanceof HDB )
      return ((HDB)dbm).errmsg();
    else if ( dbm instanceof FDB )
      return ((FDB)dbm).errmsg();

    return "";
  }
}
