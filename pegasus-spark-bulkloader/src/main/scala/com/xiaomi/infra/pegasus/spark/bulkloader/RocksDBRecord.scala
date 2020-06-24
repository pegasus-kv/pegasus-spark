package com.xiaomi.infra.pegasus.spark.bulkloader

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import com.google.common.primitives.Bytes
import org.apache.commons.lang3.Validate
import org.apache.commons.lang3.builder.HashCodeBuilder

object RocksDBRecord {
  private val epoch_begin = 1451606400 // seconds since 2016.01.01-00:00:00 GMT

  def generateKey(hashKey: Array[Byte], sortKey: Array[Byte]): Array[Byte] = {
    val hashKeyLen =
      if (hashKey == null) 0
      else hashKey.length
    Validate.isTrue(
      hashKeyLen < 65535,
      "length of hash key must be less than UINT16_MAX",
      new Array[AnyRef](0)
    )
    val sortKeyLen =
      if (sortKey == null) 0
      else sortKey.length
    val buf = ByteBuffer.allocate(2 + hashKeyLen + sortKeyLen)
    buf.putShort(hashKeyLen.toShort)
    if (hashKeyLen > 0) buf.put(hashKey)

    if (sortKeyLen > 0) buf.put(sortKey)

    buf.array
  }

  def generateValue(value: Array[Byte]): Array[Byte] = generateValue(value, 0)

  def generateValue(value: Array[Byte], ttl: Int): Array[Byte] = {
    if (ttl != 0) Bytes.concat(toBeBytes(ttl + epoch_now.toInt), value)
    else Bytes.concat(toBeBytes(ttl), value)
  }

  def epoch_now: Long = {
    val d = new Date
    d.getTime / 1000 - epoch_begin
  }

  def toBeBytes(i: Int): Array[Byte] = {
    val b = ByteBuffer.allocate(4)
    b.order(ByteOrder.BIG_ENDIAN)
    b.putInt(i)
    b.array
  }

  def create(hashKey: String, sortKey: String, value: String): RocksDBRecord = {
    new RocksDBRecord(
      generateKey(hashKey.getBytes, sortKey.getBytes),
      generateValue(value.getBytes)
    )
  }

  def create(
      hashKey: Array[Byte],
      sortKey: Array[Byte],
      value: Array[Byte]
  ): RocksDBRecord = {
    new RocksDBRecord(generateKey(hashKey, sortKey), generateValue(value))
  }

}

case class RocksDBRecord private (key: Array[Byte], value: Array[Byte]) {

  override def hashCode: Int = new HashCodeBuilder().append(key).hashCode

  override def equals(other: Any): Boolean = {
    other match {
      case that: RocksDBRecord => key.sameElements(that.key)
      case _                   => false
    }
  }

}
