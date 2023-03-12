package org.apache.spark.examples.sql.kaihua

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.kaihua.obliop.collection.FbsVector
import org.kaihua.obliop.interfaces.ObliOp
import org.kaihua.obliop.operator.Operation
import org.kaihua.obliop.operator.context.Context

import scala.collection.mutable

case class ObliviousSort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan
) extends UnaryExecNode() {

  override protected def withNewChildInternal(
      newChild: SparkPlan
  ): SparkPlan = {
    copy(child = newChild)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // oblivious sort implement
    // collect all records and divide to N Block
    var fbs = FbsVector.createVec()
    var BlockInfoList = mutable.Queue.fill[BlockInfo](1) {
      new BlockInfo(fbs)
    }
    var currentBlock = BlockInfoList.head

    val attrs = child.output.attrs
    val childRdd = child
      .execute()
    childRdd
      .foreach(record => {
        if (!currentBlock.write(record, attrs)) {
          BlockInfoList :+= new BlockInfo(fbs)
          currentBlock = BlockInfoList.last
          currentBlock.write(record, attrs)
        }
      })
    BlockInfoList.last.finish()

    // quick sort in trusted application
    BlockInfoList.foreach(block => {
      val data = FbsVector.toObliData(block.getBytBuf())
      ObliOp.ObliDataSend(data)
      val ctx = Context.empty()
      val result = Operation.sort(ctx, data);
      ObliOp.ObliOpCtxExec(ctx);
      val bytBuf = ObliOp.ObliDataGet(result)
      block.setBytBuf(bytBuf.get())
    })

    // don't need sorting network
    if (BlockInfoList.length < 1) {
      return childRdd.mapPartitionsInternal { _ =>
        new Iterator[InternalRow] {

          var bList = BlockInfoList
          var cur = if (bList.nonEmpty) {
            new blockIter(bList.dequeue())
          } else {
            null
          }

          override def hasNext: Boolean = {
            if (cur == null) {
              return false
            }
            if (!cur.hasNext) {
              if (bList.isEmpty) {
                cur = null
                return false
              }
              cur = new blockIter(bList.dequeue())
            }
            true
          }

          override def next(): InternalRow = {
            cur.next()
          }
        }
      }
    }

    // sort blocks using sorting network
    null
  }

  override def output: Seq[Attribute] = child.output

}
