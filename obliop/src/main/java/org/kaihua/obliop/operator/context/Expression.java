package org.kaihua.obliop.operator.context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.kaihua.obliop.data.ObliData;

public class Expression {
  // public for serialization
  // @todo uuid can't be random;
  public String id = UUID.randomUUID().toString();
  public ExprType typ;
  public ObliData output;
  public List<Expression> children = new ArrayList<>();
  public HashMap<ExtraExprInfo, String> info = new HashMap<>();

  public static Expression dummy() {
    return new Expression();
  }
}
