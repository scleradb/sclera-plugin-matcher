/**
* Sclera - Regular Expression Matcher
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.plugin.analytics.sequence.matcher

import scala.collection.mutable

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.expr.ScalColValue
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.ScalTableRow
import com.scleradb.sql.exec.ScalExprEvaluator

class PartialMatchesIter(
    evaluator: ScalExprEvaluator,
    pmInit: PartialMatches,
    rowLabels: ScalTableRow => List[Label],
    inputPartnCols: List[Column],
    input: Iterator[ScalTableRow]
) extends Iterator[PartialMatches] {
    private var pos: Long = 0L
    private val lookAhead: mutable.Map[
        List[ScalColValue], (PartialMatches, ScalTableRow, Long)
    ] = mutable.Map()

    override def hasNext: Boolean = input.hasNext || !lookAhead.isEmpty

    override def next: PartialMatches =
        if( input.hasNext ) {
            val row: ScalTableRow = input.next

            val partnVals: List[ScalColValue] = 
                inputPartnCols.map { col => row.getScalExpr(col) }

            val partialMatches: PartialMatches =
                lookAhead.get(partnVals) match {
                    case Some((prevpm, prevRow, _)) =>
                        val labels: List[Label] = rowLabels(prevRow)
                        prevpm.update(evaluator, prevRow, labels, false)

                    case None => pmInit
                }

            lookAhead += partnVals -> (partialMatches, row, pos)
            pos = pos + 1L

            partialMatches
        } else if( lookAhead.isEmpty ) {
            Iterator.empty.next
        } else {
            // handle last rows for each partition
            val (partnVals, (prevpm, lastRow, _)) =
                lookAhead.minBy { case (_, (_, _, p)) => p }
            lookAhead -= partnVals

            val labels: List[Label] = rowLabels(lastRow)
            prevpm.update(evaluator, lastRow, labels, true)
        }
}
