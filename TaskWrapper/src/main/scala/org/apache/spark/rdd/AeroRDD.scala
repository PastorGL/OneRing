package org.apache.spark.rdd

import java.util.Collections

import com.aerospike.client.AerospikeClient
import com.aerospike.client.query.{RecordSet, Statement}
import io.github.pastorgl.aqlselectex.AQLSelectEx
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

private[spark] class AeroPartition extends Partition {
  override def index: Int = 0
}

class AeroRDD[T: ClassTag](
                            sc: SparkContext,
                            aeroHost: String,
                            aeroPort: Int,
                            path: String)
  extends RDD[T](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    Array(new AeroPartition)
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
    context.addTaskCompletionListener[Unit] { _ => closeIfNeeded() }

    val client = new AerospikeClient(aeroHost, aeroPort)
    val select: AQLSelectEx = AQLSelectEx.forSchema(Collections.emptyMap[String, java.util.Map[String, Integer]])

    var stmt: Statement = _
    try stmt = select.fromString(path)
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }

    var rs: RecordSet = client.query(null, stmt)

    override def getNext(): T = {
      if (rs.next()) {
        rs.getRecord.bins.values.toArray.asInstanceOf[T]
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close() {
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
    }
  }
}
