package com.xiaomi.infra.pegasus.analyser;

import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.RocksIterator;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class PegasusRecord implements Serializable, Comparable {
  private byte[] hashKey;
  private byte[] sortKey;
  private byte[] value;

  public byte[] getHashKey() {
    return hashKey;
  }

  public byte[] getSortKey() {
    return sortKey;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format(
        "[HashKey=%s, SortKey=%s, Value=%s]",
        Arrays.toString(hashKey), Arrays.toString(sortKey), Arrays.toString(value));
  }

  PegasusRecord(RocksIterator rocksIterator) {
    Pair<byte[], byte[]> keyPair = restoreKey(rocksIterator.key());
    hashKey = keyPair.getLeft();
    sortKey = keyPair.getRight();
    value = restoreValue(rocksIterator.value());
  }

  private static Pair<byte[], byte[]> restoreKey(byte[] key) {
    Validate.isTrue(key != null && key.length >= 2);
    ByteBuffer buf = ByteBuffer.wrap(key);
    int hashKeyLen = 0xFFFF & buf.getShort();
    Validate.isTrue(hashKeyLen != 0xFFFF && (2 + hashKeyLen <= key.length));
    return new ImmutablePair<byte[], byte[]>(
        Arrays.copyOfRange(key, 2, 2 + hashKeyLen),
        Arrays.copyOfRange(key, 2 + hashKeyLen, key.length));
  }

  private static byte[] restoreValue(byte[] value) {
    return Arrays.copyOfRange(value, 4, value.length);
  }

  @Override
  public int compareTo(Object o) {
    PegasusRecord other = (PegasusRecord) o;
    final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
    int res = comparator.compare(hashKey, other.hashKey);
    if (res != 0) {
      return res;
    }
    return comparator.compare(sortKey, other.sortKey);
  }
}
