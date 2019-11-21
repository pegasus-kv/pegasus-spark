package com.xiaomi.infra.pegasus.analyser

import org.apache.spark.TaskContext
import org.rocksdb.{RocksDB, RocksIterator}

/**
  * Iterator of a pegasus partition. It's used to load the entire partition sequentially
  * and sorted by bytes-order.
  */
private[analyser] class PartitionIterator private (context: TaskContext,
                                                   pid: Int)
    extends Iterator[PegasusRecord] {

  private var rocksIterator: RocksIterator = _
  private var rocksDB: RocksDB = _
  private var rocksDBOptions: RocksDBOptions = _

  private var closed = false
  private var thisRecord: PegasusRecord = _
  private var nextRecord: PegasusRecord = _

  def this(context: TaskContext,
           config: Config,
           fdsService: FDSService,
           pid: Int) {
    this(context, pid)

    rocksDBOptions = new RocksDBOptions(config)

    val checkPointUrls = fdsService.getCheckpointUrls
    val dbPath = checkPointUrls.get(pid)
    rocksDB = RocksDB.openReadOnly(rocksDBOptions.options, dbPath)

    rocksIterator = rocksDB.newIterator(rocksDBOptions.readOptions)
    if (rocksIterator.isValid) {
      thisRecord = new PegasusRecord(rocksIterator)

      rocksIterator.next()
      if (rocksIterator.isValid) {
        nextRecord = new PegasusRecord(rocksIterator)
      }
    }

    // Register an on-task-completion callback to release the resources.
    context.addTaskCompletionListener { context =>
      close()
    }
  }

  private def close() {
    if (!closed) {
      // release the C++ pointers
      rocksIterator.close()
      rocksDB.close()
      rocksDBOptions.close()
      closed = true
    }
  }

  override def hasNext: Boolean = {
    nextRecord != null && !closed
  }

  override def next(): PegasusRecord = {
    thisRecord = nextRecord
    rocksIterator.next()
    if (rocksIterator.isValid) {
      nextRecord = new PegasusRecord(rocksIterator)
    } else {
      nextRecord = null
    }
    thisRecord
  }
}
