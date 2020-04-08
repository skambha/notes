/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DecimalType.SYSTEM_DEFAULT

case class DecimalSum2(child: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(DecimalType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function decimalsum")

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _ => SYSTEM_DEFAULT
  }

  private lazy val sumDataType = resultType

  private lazy val sum = AttributeReference("sum", sumDataType)()

  private lazy val isEmpty = AttributeReference("isEmpty", BooleanType)()

  private lazy val zero = Literal.default(sumDataType)

  override lazy val aggBufferAttributes = sum :: isEmpty :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* sum = */ zero,
    /* isEmpty = */ Literal.create(true, BooleanType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      Seq(
        /* sum = */
        If(IsNull(sum), sum,
          If(IsNotNull(child.cast(sumDataType)),
            CheckOverflow(sum + child.cast(sumDataType), sumDataType, true), sum)),
        /* isEmpty = */
        false
      )
    } else {
      Seq(
        /* sum = */
        If(IsNull(sum), sum, CheckOverflow(sum + child.cast(sumDataType), sumDataType, true)),
        /* isEmpty = */
        false
      )
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* sum = */
      If(And(IsNull(sum.left), EqualTo(isEmpty.left, false)) ||
        And(IsNull(sum.right), EqualTo(isEmpty.right, false)),
        Literal.create(null, resultType),
        CheckOverflow(sum.left + sum.right, resultType, true)),
      /* isEmpty = */
      And(isEmpty.left, isEmpty.right)
    )
  }

  override lazy val evaluateExpression: Expression = {
    If(EqualTo(isEmpty, true),
      Literal.create(null, resultType),
      If(And(SQLConf.get.ansiEnabled, IsNull(sum)),
        OverflowException(resultType, "Arithmetic Operation overflow"), sum))
  }
}
