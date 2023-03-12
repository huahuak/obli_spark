package org.apache.spark.examples.sql.kaihua

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
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
      // step 1. oblivious sort encrypted table and sort insecure table.
      val sortedLeft =
        SortExec(leftKeys.map(SortOrder(_, Ascending)), false, planLater(left))
      val sortedRight = ObliviousSort(
        rightKeys.map(SortOrder(_, Ascending)),
        false,
        planLater(right)
      )
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
