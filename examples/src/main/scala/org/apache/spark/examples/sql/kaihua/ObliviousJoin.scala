package org.apache.spark.examples.sql.kaihua

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Ascending,
  Attribute,
  Expression,
  SortOrder
}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{
  ClusteredDistribution,
  Distribution,
  UnspecifiedDistribution
}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.kaihua.obliop.collection.FbsVector
import org.kaihua.obliop.interfaces.ObliOp
import org.kaihua.obliop.operator.Operation
import org.kaihua.obliop.operator.context.{Context, JoinKeyInfo}

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable

case class ObliviousJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false
) extends BaseJoinExec {
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan
  ): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }

//  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
//    leftKeys.map(SortOrder(_, Ascending)) :: Nil :: Nil
//
//  override def requiredChildDistribution: Seq[Distribution] = {
//    if (isSkewJoin) {
//      // We re-arrange the shuffle partitions to deal with skew join, and the new children
//      // partitioning doesn't satisfy `HashClusteredDistribution`.
//      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
//    } else {
//      ClusteredDistribution(leftKeys) :: UnspecifiedDistribution :: Nil
//    }
//  }

  override protected def doExecute(): RDD[InternalRow] = {
    val joinId = UUID.randomUUID().toString
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val leftAttrs = left.output.attrs
      var leftBlockInfoList = mutable.Queue.fill(1) {
        new BlockInfo(leftAttrs)
      }

      val rightAttrs = right.output.attrs
      var rightBlockInfoList = mutable.Queue.fill(1) {
        new BlockInfo(rightAttrs)
      }

      var currentBlock = leftBlockInfoList.head
      leftIter.foreach(ele => {
        if (!currentBlock.write(ele)) {
          leftBlockInfoList :+= new BlockInfo(leftAttrs)
          currentBlock = leftBlockInfoList.last
          currentBlock.write(ele)
        }
      })
      leftBlockInfoList.last.finish()

      currentBlock = rightBlockInfoList.head
      rightIter.foreach(ele => {
        if (!currentBlock.write(ele)) {
          rightBlockInfoList :+= new BlockInfo(rightAttrs)
          currentBlock = rightBlockInfoList.last
          currentBlock.write(ele)
        }
      })
      rightBlockInfoList.last.finish()

      var resultBlockInfoList = mutable.Queue[BlockInfo]()

      val joinKeyInfo = List(new JoinKeyInfo(0, 0))

      // @audit this is a patch to pad list!!!
      while (rightBlockInfoList.length < leftBlockInfoList.length) {
        val empty = new BlockInfo(right.output)
        empty.finish()
        rightBlockInfoList :+= empty
      }
      while (leftBlockInfoList.length < rightBlockInfoList.length) {
        val empty = new BlockInfo(right.output)
        empty.finish()
        leftBlockInfoList :+= empty
      }

      leftBlockInfoList
        .zip(rightBlockInfoList)
        .foreach(t => {
          val (l, r) = t
          val lhs = FbsVector.toObliData(l.getBytBuf())
          val rhs = FbsVector.toObliData(r.getBytBuf())
          val expr = Operation.equiJoin(
            Operation.newDataNode(lhs),
            Operation.newDataNode(rhs),
            joinKeyInfo.asJava
          )
          expr.id = joinId
          ObliOp.ObliDataSend(lhs)
          ObliOp.ObliDataSend(rhs)
          val ctx = Context.empty();
          ObliOp.ObliOpCtxExec(ctx.addExpr(expr))
          val result = ObliOp.ObliDataGet(expr.output).get()
//          FbsVector.printFbs(result)
          val blockInfo = new BlockInfo(left.output ++ right.output)
          blockInfo.setBytBuf(result)
          resultBlockInfoList :+= blockInfo
        })

      new Iterator[InternalRow] {
        val bList = resultBlockInfoList
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
          cur.hasNext
        }

        override def next(): InternalRow = {
          cur.next()
        }
      }
    }
  }

  override def output: Seq[Attribute] = {
    left.output ++ right.output
  }
}
