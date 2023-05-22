package org.apache.spark.examples.sql.kaihua

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{
  Ascending,
  Expression,
  SortOrder
}
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{
  ClusteredDistribution,
  Distribution
}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{SortExec, SparkPlan}

object ObliviousJoinStrategy extends Strategy with JoinSelectionHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case j @ ExtractEquiJoinKeys(
          joinType,
          leftKeys,
          rightKeys,
          nonEquiCond,
          _,
          left,
          right,
          hint
        ) =>
      val partitionSize = 200
      val exLeft = ShuffleExchangeExec(
        ClusteredDistribution(leftKeys).createPartitioning(partitionSize),
        planLater(left)
      )
      val exRight = ShuffleExchangeExec(
        ClusteredDistribution(rightKeys).createPartitioning(partitionSize),
        planLater(right)
      )
      // step 1. oblivious sort encrypted table and sort insecure table.
      val sortedLeft =
        SortExec(leftKeys.map(SortOrder(_, Ascending)), false, exLeft)

      val sortedRight =
        SortExec(rightKeys.map(SortOrder(_, Ascending)), false, exRight)
//      val sortedRight = ObliviousSort(
//        rightKeys,
//        rightKeys.map(SortOrder(_, Ascending)),
//        false,
//        exRight
//      )
      // step 2. oblivious join twos.
      val obliJoin = ObliviousJoin(
        leftKeys,
        rightKeys,
        joinType,
        nonEquiCond,
        sortedLeft,
        sortedRight
      )
      // step 3. oblivious sort dummy record and filter

      Seq(obliJoin)

    case _ => Nil
  }
}
