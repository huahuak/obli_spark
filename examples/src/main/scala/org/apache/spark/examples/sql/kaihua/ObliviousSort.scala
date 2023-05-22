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
import org.apache.spark.sql.catalyst.plans.physical.{
  ClusteredDistribution,
  Distribution
}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.kaihua.obliop.collection.FbsVector
import org.kaihua.obliop.interfaces.ObliOp
import org.kaihua.obliop.operator.Operation
import org.kaihua.obliop.operator.context.{Context, SortOrderInfo}

import scala.collection.mutable
import scala.jdk.CollectionConverters.seqAsJavaListConverter

case class ObliviousSort(
    keys: Seq[Expression],
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
    val ObliviousSortFunction = (iter: Iterator[InternalRow]) => {
      val attrs = child.output.attrs
      var BlockInfoList = mutable.Queue.fill[BlockInfo](1) {
        new BlockInfo(attrs)
      }
      var currentBlock = BlockInfoList.head
      // collect all records and divide to N Block
      iter.foreach(record => {
        if (!currentBlock.write(record)) {
          BlockInfoList :+= new BlockInfo(attrs)
          currentBlock = BlockInfoList.last
          currentBlock.write(record)
        }
      })
      BlockInfoList.last.finish()

      // don't need sorting network
      def nonSortingNetwork(): Unit = {
        assert(
          BlockInfoList.length == 1,
          "only one block can use nonSortingNetworks! but has %d blocks" format BlockInfoList.length
        )
        val block = BlockInfoList.head
        val data = FbsVector.toObliData(block.getBytBuf())
        ObliOp.ObliDataSend(data)
        val ctx = Context.empty()
        // get sort order
        var sortOrderList: List[SortOrderInfo] = List()
        sortOrder.foreach(so => {
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
          Operation.sort(Operation.newDataNode(data), sortOrderList.asJava)
        ctx.addExpr(result)
        ObliOp.ObliOpCtxExec(ctx)
        val bytBuf = ObliOp.ObliDataGet(result.output)
        block.setBytBuf(bytBuf.get())
      }

      // Check if the input data size is a power of 2,
      // append dummy data if it isn't power of 2
      def sortingNetworkEnv(function: Array[BlockInfo] => Unit): Unit = {
        // let input size be power of 2
        val inputSize = Iterator
          .iterate(1)(_ << 1)
          .takeWhile(_ < BlockInfoList.length)
          .toSeq
          .last << 1
        val originalSize = BlockInfoList.length
        var in = Array.fill(originalSize)(BlockInfoList.dequeue)
        while (in.length < inputSize) {
          // null attrs block is dummy block,
          // which is bigger than any block
          in :+= BlockInfo.dummy()
        }
        function(in)
        (0 until originalSize).foreach(index => {
          BlockInfoList :+= in(index)
        })
      }

      // sort blocks using sorting network
      def sortingNetwork(blocks: Array[BlockInfo]): Unit = {
        def obliviousCompareAndSwap(
            lIndex: Int,
            rIndex: Int,
            directionInSortingNetwork: Int
        ): Unit = {
          // get sort order
          var sortOrderList: List[SortOrderInfo] = List()
          sortOrder.foreach(so => {
            // val soAttr = so.asInstanceOf[Expression].references;
            attrs.zipWithIndex.foreach(tuple => {
              val (a, index) = tuple
              if (so.child.canonicalized.equals(a.canonicalized)) {
                // noinspection ScalaStyle
                val direction = so.direction match {
                  case Ascending  => 1
                  case Descending => -1
                }
                sortOrderList :+= new SortOrderInfo(
                  index,
                  direction * directionInSortingNetwork
                )
              }
            })
          })

          val lhs = blocks(lIndex)
          val rhs = blocks(rIndex)
          // the dummy block is infinite big.
          // if found the dummy block, simply cmp and swap
          if (rhs.isDummy) {
            return
          }
          if (lhs.isDummy) {
            blocks(lIndex) = rhs
            blocks(rIndex) = lhs
            return
          }
          // merge two blocks into one block,
          // then sort one block in TEE,
          // finally restore to original two blocks
          val unSortedBlock = BlockInfo.merge(lhs, rhs)
          val obliData = FbsVector.toObliData(unSortedBlock.getBytBuf())
          ObliOp.ObliDataSend(obliData)
          val ctx = Context.empty()
          val sortedObliData = {
            Operation.sort(
              Operation.newDataNode(obliData),
              sortOrderList.asJava
            )
          }
          ctx.addExpr(sortedObliData)
          ObliOp.ObliOpCtxExec(ctx)
          val bytBuf = ObliOp.ObliDataGet(sortedObliData.output)
          val sortedBlock = unSortedBlock
          sortedBlock.setBytBuf(bytBuf.get())
          BlockInfo.divideInto2Blocks(sortedBlock, lhs, rhs)
        }

        Iterator
          .iterate(2)(i => { i * 2 })
          .takeWhile(_ <= blocks.length)
          .foreach(step => {
            Iterator
              .iterate(step / 2)(i => { (i * 0.5).toInt })
              .takeWhile(_ > 0)
              .foreach(innerStep => {
                blocks.indices.foreach(index => {
                  val xor = index ^ innerStep
                  if (xor > index) {
                    val directionInSortingNetwork =
                      if ((index & step) != 0) { 1 }
                      else { -1 }
                    obliviousCompareAndSwap(
                      index,
                      xor,
                      directionInSortingNetwork
                    )
                  }
                })
              })
          })
      }

      if (BlockInfoList.length < 2) {
        nonSortingNetwork()
      } else {
        sortingNetworkEnv { sortingNetwork }
      }

      // return iterator on blocks
      new Iterator[InternalRow] {
        val bList: mutable.Queue[BlockInfo] = BlockInfoList
        var cur: blockIter = if (bList.nonEmpty) {
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
          cur.hasNext
        }

        override def next(): InternalRow = {
          cur.next()
        }
      }
    }

    child
      .execute()
      .mapPartitionsInternal(ObliviousSortFunction)
  }

  override def output: Seq[Attribute] = child.output

}
