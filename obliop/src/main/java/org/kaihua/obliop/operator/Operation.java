package org.kaihua.obliop.operator;

import java.util.List;

import org.kaihua.obliop.data.ObliData;
import org.kaihua.obliop.operator.context.Context;
import org.kaihua.obliop.operator.context.ExprType;
import org.kaihua.obliop.operator.context.Expression;
import org.kaihua.obliop.operator.context.ExtraExprInfo;
import org.kaihua.obliop.operator.context.JoinKeyInfo;
import org.kaihua.obliop.operator.context.SortOrderInfo;

import com.google.gson.Gson;

public class Operation {
  public static Expression hash(Expression in) {
    return newOpNode(in, ExprType.HASH);
  }

  public static Expression mod(Expression in) {
    return newOpNode(in, ExprType.MOD);
  }

  public static Expression sort(Expression in, List<SortOrderInfo> orders) {
    assert in != null : "input data is null";
    Expression expr = newOpNode(in, ExprType.SORT);
    Gson gson = new Gson();
    expr.info.put(ExtraExprInfo.SortOrder, gson.toJson(orders));
    return expr;
  }

  public static Expression equiJoin(Expression lhs, Expression rhs, List<JoinKeyInfo> joinKeyInfos) {
    assert lhs != null : "input lhs data is null";
    assert rhs != null : "input rhs data is null";

    Expression expr = newOpNode(lhs, ExprType.EQUIJOIN);
    expr.children.add(rhs);
    Gson gson = new Gson();
    expr.info.put(ExtraExprInfo.EquiJoinKey, gson.toJson(joinKeyInfos));
    return expr;
  }

  public static Expression newDataNode(ObliData in) {
    assert in != null : "input data is null";
    Expression expr = new Expression();
    expr.typ = ExprType.DATA;
    expr.output = in;
    return expr;
  }

  private static Expression newOpNode(Expression in, ExprType typ) {
    assert in != null : "input data is null";
    ObliData out = ObliData.empty();
    Expression expr = new Expression();
    expr.typ = typ;
    expr.children.add(in);
    expr.output = out;
    return expr;
  }
}
