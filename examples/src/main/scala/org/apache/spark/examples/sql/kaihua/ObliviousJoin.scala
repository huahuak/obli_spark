package org.apache.spark.examples.sql.kaihua

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.kaihua.obliop.collection.FbsVector
import org.kaihua.obliop.interfaces.ObliOp
import org.kaihua.obliop.operator.Operation
import org.kaihua.obliop.operator.context.{Context, JoinKeyInfo}

import scala.collection.mutable
import collection.JavaConverters._

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

  override protected def doExecute(): RDD[InternalRow] = {
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

      val resultBlockInfoList = mutable.Queue.fill(1) {
        new BlockInfo(left.output ++ right.output)
      }

      // ------------------------------------ //
      // @todo only one block join for now
      val lhs = FbsVector.toObliData(leftBlockInfoList.head.getBytBuf())
      val rhs = FbsVector.toObliData(rightBlockInfoList.head.getBytBuf())
      val joinKeyInfo = List(new JoinKeyInfo(0, 1))
      val expr = Operation.equiJoin(
        Operation.newDataNode(lhs),
        Operation.newDataNode(rhs),
        joinKeyInfo.asJava
      )
      ObliOp.ObliDataSend(lhs)
      ObliOp.ObliDataSend(rhs)
      ObliOp.ObliOpCtxExec(Context.empty().addExpr(expr))
      val result = ObliOp.ObliDataGet(expr.output).get()
      FbsVector.printFbs(result)

      resultBlockInfoList.head.setBytBuf(result)
      // ------------------------------------ //

      new Iterator[InternalRow] {
        var bList = resultBlockInfoList
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

  override def output: Seq[Attribute] = {
    left.output ++ right.output
  }
}
