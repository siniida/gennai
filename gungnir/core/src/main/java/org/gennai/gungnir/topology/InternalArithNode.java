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

package org.gennai.gungnir.topology;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.Set;

import org.gennai.gungnir.tuple.FieldAccessor;

import scala.collection.mutable.StringBuilder;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class InternalArithNode extends BaseArithNode {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private ArithNode leftNode;
  private ArithNode rightNode;
  private Operator operatior = Operator.NONE;

  public InternalArithNode(Operator operator, ArithNode leftNode,
      ArithNode rightNode) {
    this.operatior = operator;
    this.leftNode = leftNode;
    this.rightNode = rightNode;
  }

  public Operator getOperatior() {
    return operatior;
  }

  public ArithNode getLeftNode() {
    return leftNode;
  }

  public ArithNode getRightNode() {
    return rightNode;
  }

  @Override
  public List<FieldAccessor> getFields() {
    Set<FieldAccessor> fields = Sets.newLinkedHashSet();
    List<FieldAccessor> leftFields = leftNode.getFields();
    List<FieldAccessor> rightFields = rightNode.getFields();
    if (leftFields != null) {
      fields.addAll(leftFields);
    }
    if (rightFields != null) {
      fields.addAll(rightFields);
    }
    return Lists.newArrayList(fields);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (leftNode.isInternal()) {
      sb.append('(');
    }
    sb.append(leftNode);
    if (leftNode.isInternal()) {
      sb.append(')');
    }
    sb.append(' ');
    sb.append(operatior.getDisplayString());
    sb.append(' ');
    if (rightNode.isInternal()) {
      sb.append('(');
    }
    sb.append(rightNode);
    if (rightNode.isInternal()) {
      sb.append(')');
    }
    return sb.toString();
  }
}
