package org.apache.spark.examples.sql.kaihua

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.kaihua.obliop.collection.FbsVector
import org.kaihua.obliop.interfaces.ObliOp
import org.kaihua.obliop.operator.Operation
import org.kaihua.obliop.operator.context.Context

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
    val BlockMaxSize = math.pow(10, 4)
    // collect all records and divide to N Block
    var BlockInfoList = Array.fill[BlockInfo](1) {
      new BlockInfo
    }
    var currentBlock = BlockInfoList(0)

    var cnt = 0
    child
      .execute()
      .foreach(record => {
        if (currentBlock.len > BlockMaxSize) {
          BlockInfoList :+= new BlockInfo
          cnt = 0 // resize counter
        }
        cnt += 1
        currentBlock.write(record)
      })

    // quick sort in trusted application
    BlockInfoList.foreach(item => {
      var fbs = FbsVector.createVec()
      val bytbuf = fbs.finish()
      val data = FbsVector.toObliData(bytbuf)
      ObliOp.ObliDataSend(data)

      val ctx = Context.empty();
      val result = Operation.mod(ctx, Operation.hash(ctx, data));
      ObliOp.ObliOpCtxExec(ctx);
    })

    // sort blocks using sorting network
    if (BlockInfoList.length > 1) {

    }
    null
  }

  override def output: Seq[Attribute] = child.output
}

class BlockInfo {
  def write(internalRow: InternalRow): Unit = {}
  def len(): Int = { 0 }
}
