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

import com.scleradb.sql.expr.{ScalExpr, ColRef, SortExpr}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow}
import com.scleradb.sql.exec.ScalExprEvaluator

import com.scleradb.util.automata.datatypes.Label
import com.scleradb.util.automata.nfa.AnchoredNfa

import com.scleradb.analytics.sequence.matcher.aggregate._
import com.scleradb.analytics.sequence.matcher.RowSequenceMatcher

class NfaMatcher(
    val anchoredNfa: AnchoredNfa,
    override val aggregateRowsSpec: SeqAggregateRowsSpec,
    override val partitionCols: List[ColRef]
) extends RowSequenceMatcher {
    override def tableColRefs(inpTableCols: List[ColRef]): List[ColRef] =
        aggregateRowsSpec.resultColRefs(inpTableCols)

    override def matchResult(
        evaluator: ScalExprEvaluator,
        rowLabels: ScalTableRow => List[Label],
        input: TableResult
    ): TableResult = {
        val aggregates: SeqAggregateRows = aggregateRowsSpec.aggregate(input)
        
        val inputPartnCols: List[Column] = partitionCols.map { colRef =>
            input.columnOpt(colRef.name).getOrElse {
                throw new IllegalArgumentException(
                    "Partition column \"" + colRef.repr + "\" not found"
                )
            }
        }

        new MatchTableResult(
            evaluator, anchoredNfa, aggregates, rowLabels,
            inputPartnCols, input, resultOrder(input.resultOrder)
        )
    }

    override def resultOrder(
        inpOrder: List[SortExpr]
    ): List[SortExpr] = aggregateRowsSpec match {
        case SeqArgOptsSpec(aggregateSpecs) =>
            inpOrder.takeWhile { case SortExpr(expr, _, _) =>
                partitionCols contains expr
            }

        case SeqAggregateColSetSpec(aggregateColSpecs, retainedCols) =>
            val (partnOrder, remOrder) =
                inpOrder.span { case SortExpr(expr, _, _) =>
                    partitionCols contains expr
                }

            val partnOrderExprs: List[ScalExpr] =
                partnOrder map { case SortExpr(expr, _, _) => expr }

            if( partnOrderExprs.distinct.size == partitionCols.distinct.size ) {
                // input comping sorted in partition order
                val retainedOrder: List[SortExpr] =
                    remOrder.takeWhile { case SortExpr(expr, _, _) =>
                        retainedCols contains expr
                    }

                partnOrder ::: retainedOrder
            } else {
                val aggregateColPairs: List[(ColRef, ColRef)] =
                    aggregateColSpecs.flatMap {
                        case SeqColumnSpec(alias, col, None, Nil)
                        if partitionCols contains col => Some(col -> alias)
                        case _ => None
                    }

                val retainedColPairs: List[(ColRef, ColRef)] =
                    retainedCols.flatMap { col =>
                        if( partitionCols contains col ) Some(col -> col)
                        else None
                    }

                val colMap: Map[ScalExpr, ColRef] = Map[ScalExpr, ColRef]() ++
                    aggregateColPairs ++ retainedColPairs

                inpOrder.takeWhile { case SortExpr(expr, _, _) =>
                    colMap contains expr
                } map { case SortExpr(expr, sortDir, nullsOrder) =>
                    SortExpr(colMap(expr), sortDir, nullsOrder)
                }
            }
    }

    override def clone(
        newAggregateSpec: SeqAggregateRowsSpec,
        newPartitionCols: List[ColRef]
    ): NfaMatcher =
        new NfaMatcher(anchoredNfa, newAggregateSpec, newPartitionCols)

    override def toString: String =
        "NfaMatcher(%s,%s,%s)".format(
            anchoredNfa, aggregateRowsSpec, partitionCols
        )

    override def repr: List[String] =
        ("[" + anchoredNfa.toString + "]")::super.repr
}
