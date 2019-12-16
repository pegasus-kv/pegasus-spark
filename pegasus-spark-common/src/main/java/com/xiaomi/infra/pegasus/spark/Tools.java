package com.xiaomi.infra.pegasus.spark;

import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;

public class Tools {

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Tools.class);

  private static class dsn_crc {

    public static final long crc64_poly = 0x9a6c9329ac4bc9b5l;
    public static final int crc32_poly = 0x82f63b78;
    public static final int crc32_table[] = new int[0x100];
    public static final long crc64_table[] = new long[0x100];

    static {
      for (int i = 0; i < 256; ++i) {
        int k1 = i;
        long k2 = (long) i;
        for (int j = 0; j < 8; ++j) {
          if ((k1 & 1) == 1) {
            k1 = (k1 >>> 1) ^ crc32_poly;
          } else {
            k1 = (k1 >>> 1);
          }

          if ((k2 & 1) == 1) {
            k2 = (k2 >>> 1) ^ crc64_poly;
          } else {
            k2 = (k2 >>> 1);
          }
        }
        crc32_table[i] = k1;
        crc64_table[i] = k2;
      }
    }
  }

  public static int dsn_crc32(byte[] array) {
    return dsn_crc32(array, 0, array.length);
  }

  public static int dsn_crc32(byte[] array, int offset, int length) {
    int crc = -1;
    int end = offset + length;
    for (int i = offset; i < end; ++i) {
      crc = dsn_crc.crc32_table[(array[i] ^ crc) & 0xFF] ^ (crc >>> 8);
    }
    return ~crc;
  }

  public static long dsn_crc64(byte[] array) {
    return dsn_crc64(array, 0, array.length);
  }

  public static long dsn_crc64(byte[] array, int offset, int length) {
    long crc = -1;
    int end = offset + length;
    for (int i = offset; i < end; ++i) {
      crc = dsn_crc.crc64_table[(array[i] ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    }
    return ~crc;
  }

  public static long remainderUnsigned(long dividend, long divisor) {
    if (dividend > 0L) {
      return dividend % divisor;
    } else {
      long reminder = (dividend >>> 1) % divisor * 2L + (dividend & 1L);
      return reminder >= 0L && reminder < divisor ? reminder : reminder - divisor;
    }
  }

  public static int compare(byte[] byteArray1, byte[] byteArray2) {
    if (byteArray1 == byteArray2) {
      return 0;
    }

    if (byteArray1 == null) {
      if (byteArray2 == null) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (byteArray2 == null) {
        return 1;
      } else {
        if (byteArray1.length < byteArray2.length) {
          int pos = 0;

          for (byte b1 : byteArray1) {
            byte b2 = byteArray2[pos];

            if (b1 == b2) {
              pos++;
            } else if (b1 < b2) {
              return -1;
            } else {
              return 1;
            }
          }

          return -1;
        } else {
          int pos = 0;

          for (byte b2 : byteArray2) {
            byte b1 = byteArray1[pos];

            if (b1 == b2) {
              pos++;
            } else if (b1 < b2) {
              return -1;
            } else {
              return 1;
            }
          }

          if (pos < byteArray1.length) {
            return 1;
          } else {
            return 0;
          }
        }
      }
    }
  }

  public static long hash(byte[] pegasusKey) {
    Validate.isTrue(pegasusKey != null && pegasusKey.length >= 2);
    ByteBuffer buf = ByteBuffer.wrap(pegasusKey);
    int hashKeyLen = 0xFFFF & buf.getShort();
    Validate.isTrue(hashKeyLen != 0xFFFF && (2 + hashKeyLen <= pegasusKey.length));
    return hashKeyLen == 0
        ? Tools.dsn_crc64(pegasusKey, 2, pegasusKey.length - 2)
        : Tools.dsn_crc64(pegasusKey, 2, hashKeyLen);
  }
}
