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

import com.scleradb.sql.exec.ScalExprEvaluator
import com.scleradb.sql.expr.{ColRef, SortExpr, SqlNull}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow}
import com.scleradb.sql.result.TableRowGroupIterator

import com.scleradb.util.automata.nfa.AnchoredNfa
import com.scleradb.util.automata.datatypes.Label

import com.scleradb.analytics.sequence.matcher.aggregate.SeqAggregateRows

class MatchTableResult(
    evaluator: ScalExprEvaluator,
    anchoredNfa: AnchoredNfa,
    aggregateRows: SeqAggregateRows,
    rowLabels: ScalTableRow => List[Label],
    inpPartnCols: List[Column],
    input: TableResult,
    override val resultOrder: List[SortExpr]
) extends TableResult {
    override val columns: List[Column] = aggregateRows.columns

    override def rows: Iterator[ScalTableRow] = {
        val partnCols: List[ColRef] =
            SortExpr.compatiblePartnCols(
                input.resultOrder,
                inpPartnCols.map { col => ColRef(col.name) }
            )

        val inpRows: Iterator[ScalTableRow] = input.typedRows
        val nullRow: ScalTableRow = ScalTableRow(
            input.columns.map { col => col.name -> SqlNull(col.sqlType) }
        )

        if( inpRows.hasNext ) {
            val pmInit: PartialMatches =
                PartialMatches(anchoredNfa, aggregateRows, false)

            val pmIter: Iterator[PartialMatches] =
                if( partnCols.isEmpty ) {
                    new PartialMatchesIter(
                        evaluator, pmInit,
                        rowLabels, inpPartnCols, inpRows
                    )
                } else {
                    TableRowGroupIterator(
                        evaluator, inpRows, partnCols
                    ).flatMap { group =>
                        new PartialMatchesIter(
                            evaluator, pmInit,
                            rowLabels, inpPartnCols, group.rows
                        )
                    }
                }

            pmIter.flatMap { pm =>
                val row: ScalTableRow = pm.rowOpt getOrElse nullRow
                pm.resultOpt.iterator.flatMap { aggregateResult =>
                    aggregateResult.resultRows(row).iterator
                }
            }
        } else {
            val pm: PartialMatches =
                PartialMatches(anchoredNfa, aggregateRows, true)

            pm.resultOpt.iterator.flatMap { aggregateResult =>
                aggregateResult.resultRows(nullRow).iterator
            }
        }
    }

    override def close(): Unit = { /* empty */ }
}
