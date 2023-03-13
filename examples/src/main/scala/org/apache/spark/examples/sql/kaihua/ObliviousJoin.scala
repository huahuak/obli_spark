package org.apache.spark.examples.sql.kaihua

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}

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
    val a = right.execute()
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      new RowIterator {
        override def advanceNext(): Boolean = {
          false
        }

        override def getRow: InternalRow = {
          null
        }
      }.toScala
    }
  }

  override def output: Seq[Attribute] = {
    left.output ++ right.output
  }
}
