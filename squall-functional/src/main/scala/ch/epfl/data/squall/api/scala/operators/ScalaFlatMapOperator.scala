/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.api.scala.operators

import ch.epfl.data.squall.operators.Operator
import ch.epfl.data.squall.visitors.OperatorVisitor
import ch.epfl.data.squall.api.scala.SquallType._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._


/**
 * @author mohamed
 */
class ScalaFlatMapOperator[T: SquallType, U: SquallType](fn: T => Seq[U]) extends Operator {

  private var _numTuplesProcessed: Int = 0;

  def accept(ov: OperatorVisitor): Unit = {
    //ov.visit(this);
  }

  def getContent(): java.util.List[String] = {
    throw new RuntimeException("getContent for SelectionOperator should never be invoked!")
  }

  def getNumTuplesProcessed(): Int = {
    _numTuplesProcessed
  }

  def isBlocking(): Boolean = {
    false
  }

  def printContent(): String = {
    throw new RuntimeException("printContent for SelectionOperator should never be invoked!");
  }

  def process(tuple: java.util.List[String], lineageTimestamp: Long): java.util.List[java.util.List[String]] = {
    _numTuplesProcessed += 1;
    val squalTypeInput: SquallType[T] = implicitly[SquallType[T]]
    val squalTypeOutput: SquallType[U] = implicitly[SquallType[U]]
    val squallTuple = squalTypeInput.convertBack(tuple.toList)
    val cmp = fn(squallTuple)
    val res = cmp map { squalTypeOutput.convert(_) }
    res map { x => x : java.util.List[String] }
  }
}
