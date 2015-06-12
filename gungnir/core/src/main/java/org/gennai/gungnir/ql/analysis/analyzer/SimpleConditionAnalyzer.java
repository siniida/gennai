/**
 * Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
 * (see CONTRIBUTORS.md)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  A copy of the
 * License is distributed with this work in the LICENSE.md file.  You may
 * also obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gennai.gungnir.ql.analysis.analyzer;

import static org.gennai.gungnir.ql.analysis.GungnirLexer.*;

import java.lang.reflect.Array;

import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzer;
import org.gennai.gungnir.tuple.Condition.ConditionType;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.SimpleCondition;

public class SimpleConditionAnalyzer implements Analyzer<SimpleCondition> {

  @Override
  public SimpleCondition analyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    ConditionType type = null;
    switch (node.getType()) {
      case TOK_OP_EQ:
        type = ConditionType.EQ;
        break;
      case TOK_OP_NE:
        type = ConditionType.NE;
        break;
      case TOK_OP_LE:
        type = ConditionType.LE;
        break;
      case TOK_OP_LT:
        type = ConditionType.LT;
        break;
      case TOK_OP_GE:
        type = ConditionType.GE;
        break;
      case TOK_OP_GT:
        type = ConditionType.GT;
        break;
      case TOK_OP_LIKE:
        type = ConditionType.LIKE;
        break;
      case TOK_OP_REGEXP:
        type = ConditionType.REGEXP;
        break;
      case TOK_OP_IN:
        type = ConditionType.IN;
        break;
      case TOK_OP_ALL:
        type = ConditionType.ALL;
        break;
      case TOK_OP_BETWEEN:
        type = ConditionType.BETWEEN;
        break;
      case TOK_OP_IS_NULL:
        type = ConditionType.IS_NULL;
        break;
      case TOK_OP_IS_NOT_NULL:
        type = ConditionType.IS_NOT_NULL;
        break;
      default:
        throw new SemanticAnalyzeException("Invalid condition type '" + node.getText() + "'");
    }

    Field leftExpr = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));

    Object rightExpr = null;
    if (node.getChildCount() == 2) {
      rightExpr = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
    } else if (node.getChildCount() > 2) {
      rightExpr = new Object[node.getChildCount() - 1];
      for (int i = 1; i < node.getChildCount(); i++) {
        Array.set(rightExpr, i - 1, semanticAnalyzer.analyzeByAnalyzer(node.getChild(i)));
      }
    }
    return new SimpleCondition(type, leftExpr, rightExpr);
  }
}
