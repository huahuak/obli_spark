package org.kaihua.obliop.operator.context;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class Context {
  // public for serialization
  public List<Expression> expressions = new ArrayList<>();

  public Context addExpr(Expression expr) {
    this.expressions.add(expr);
    return this;
  }

  private static Optional<Expression> find(
      Expression root,
      Expression target,
      Comparator<Expression> cmp) {

    if (cmp.compare(root, target) == 0) {
      return Optional.of(root);
    }
    Expression cur = root;
    for (Expression child : cur.children) {
      Optional<Expression> ans = find(child, target, cmp);
      if (ans.isPresent()) {
        return ans;
      }
    }
    return Optional.empty();
  }

  public static Context empty() {
    Context ctx = new Context();
    return ctx;
  }
}
