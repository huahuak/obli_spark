package org.apache.spark.examples.sql.kaihua

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Ascending,
  Attribute,
  Descending,
  Expression,
  SortOrder
}
import org.apache.spark.sql.execution.{RowIterator, SparkPlan, UnaryExecNode}
import org.kaihua.obliop.collection.FbsVector
import org.kaihua.obliop.interfaces.ObliOp
import org.kaihua.obliop.operator.Operation
import org.kaihua.obliop.operator.context.{Context, SortOrderInfo}

import scala.collection.mutable
import scala.jdk.CollectionConverters.seqAsJavaListConverter

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
    child
      .execute()
      .mapPartitionsInternal(iter => {
        val attrs = child.output.attrs
        val fbs = FbsVector.createVec()
        var BlockInfoList = mutable.Queue.fill[BlockInfo](1) {
          new BlockInfo(fbs)
        }
        var currentBlock = BlockInfoList.head
        // collect all records and divide to N Block
        iter.foreach(record => {
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
          // get sort order
          var sortOrderList: List[SortOrderInfo] = List()
          sortOrder.foreach(so => {
//            val soAttr = so.asInstanceOf[Expression].references;
            attrs.zipWithIndex.foreach(tuple => {
              val (a, index) = tuple
              if (so.child.canonicalized.equals(a.canonicalized)) {
                // noinspection ScalaStyle
                val direction = so.direction match {
                  case Ascending  => 1
                  case Descending => -1
                }
                sortOrderList :+= new SortOrderInfo(index, direction)
              }
            })

          })
          val result =
            Operation.sort(ctx, data, sortOrderList.asJava);
          ObliOp.ObliOpCtxExec(ctx);
          val bytBuf = ObliOp.ObliDataGet(result)
          block.setBytBuf(bytBuf.get())
        })

        // don't need sorting network
        def nonSortingNetwork(): Iterator[InternalRow] = {
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

        // sort blocks using sorting network
        def sortingNetwork(): Iterator[InternalRow] = { null }

        if (BlockInfoList.length < 2) {
          nonSortingNetwork()
        } else {
          sortingNetwork()
        }
      })
  }

  override def output: Seq[Attribute] = child.output

}
