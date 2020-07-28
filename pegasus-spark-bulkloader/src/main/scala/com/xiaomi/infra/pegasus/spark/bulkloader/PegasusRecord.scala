package com.xiaomi.infra.pegasus.spark.bulkloader

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import com.google.common.primitives.Bytes
import org.apache.commons.lang3.Validate
import org.apache.commons.lang3.builder.HashCodeBuilder

object PegasusRecord {
  private val EPOCH_BEGIN = 1451606400 // seconds since 2016.01.01-00:00:00 GMT

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

  // todo(jiashuo1): ttl, ts, clusterId, deleteTag default 0, later offer api to set it
  def generateValue(
      value: Array[Byte],
      ttl: Int = 0,
      ts: Long = 0,
      clusterId: Short = 0,
      deleteTag: Byte = 0
  ): Array[Byte] = {
    val externTag = Long2Bytes(ts << 8 | clusterId << 1 | deleteTag)
    if (ttl != 0)
      Bytes.concat(Int2Bytes(ttl + epochNow.toInt), externTag, value)
    else Bytes.concat(Int2Bytes(ttl), externTag, value)
  }

  def epochNow: Long = {
    val d = new Date
    d.getTime / 1000 - EPOCH_BEGIN
  }

  def Int2Bytes(i: Int): Array[Byte] = {
    val b = ByteBuffer.allocate(4)
    b.order(ByteOrder.BIG_ENDIAN)
    b.putInt(i)
    b.array
  }

  def Long2Bytes(i: Long): Array[Byte] = {
    val b = ByteBuffer.allocate(8)
    b.order(ByteOrder.BIG_ENDIAN)
    b.putLong(i)
    b.array
  }

  def create(
      hashKey: String,
      sortKey: String,
      value: String
  ): (PegasusKey, PegasusValue) = {
    (
      PegasusKey(generateKey(hashKey.getBytes, sortKey.getBytes)),
      PegasusValue(generateValue(value.getBytes))
    )
  }

  def create(
      hashKey: Array[Byte],
      sortKey: Array[Byte],
      value: Array[Byte]
  ): (PegasusKey, PegasusValue) = {
    (
      PegasusKey(generateKey(hashKey, sortKey)),
      PegasusValue(generateValue(value))
    )
  }

}

class PegasusBytes(record: Array[Byte]) {

  val data: Array[Byte] = record

  override def hashCode: Int = new HashCodeBuilder().append(record).hashCode

  override def equals(other: Any): Boolean = {
    other match {
      case that: PegasusBytes => data.sameElements(that.data)
      case _                  => false
    }
  }
}

case class PegasusKey(key:Array[Byte]) extends PegasusBytes(record = key)
case class PegasusValue(value:Array[Byte]) extends PegasusBytes(record = value)