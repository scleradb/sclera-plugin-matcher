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

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.ScalTableRow
import com.scleradb.sql.exec.ScalExprEvaluator

import com.scleradb.analytics.sequence.matcher.aggregate.SeqAggregateRows

class SeqAggregateResult(
    val index: Long,
    aggregateRows: SeqAggregateRows
) {
    val columns: List[Column] = aggregateRows.columns

    def resultRows(curRow: ScalTableRow): List[ScalTableRow] = {
        val cnames: List[String] = columns.map { col => col.name }
        aggregateRows.result(curRow).map { vals =>
            ScalTableRow(cnames zip vals)
        }
    }

    def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqAggregateResult = new SeqAggregateResult(
        index, aggregateRows.update(evaluator, r, rLabel)
    )

    override def toString: String = "[" + index + "] " + aggregateRows.toString
}

object SeqAggregateResult {
    def apply(
        index: Long,
        aggregateRows: SeqAggregateRows
    ): SeqAggregateResult = new SeqAggregateResult(index, aggregateRows)
}
