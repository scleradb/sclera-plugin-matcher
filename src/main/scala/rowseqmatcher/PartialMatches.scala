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

import com.scleradb.sql.result.ScalTableRow
import com.scleradb.sql.exec.ScalExprEvaluator

import com.scleradb.util.automata.nfa.{Nfa, AnchoredNfa}
import com.scleradb.util.automata.datatypes.{State, Label}

import com.scleradb.analytics.sequence.matcher.aggregate._

// contains partial aggregates, affects NFA traversal
class PartialMatches(
    nfa: AnchoredNfa,
    aggregates: SeqAggregateRows,
    stateMap: Map[State, SeqAggregateResult],
    index: Long,
    val rowOpt: Option[ScalTableRow],
    isEnd: Boolean
) {
    def update(
        evaluator: ScalExprEvaluator,
        row: ScalTableRow,
        rowLabels: List[Label],
        isLastRow: Boolean
    ): PartialMatches = {
        val startStateMap: Map[State, SeqAggregateResult] =
            if( nfa.isAnchoredBegin ) Map()
            else Map(nfa.startState -> SeqAggregateResult(index, aggregates))

        val nextStateMap: Map[State, SeqAggregateResult] = startStateMap ++ {
            nfa.labelStates(rowLabels).flatMap { state =>
                // partial matches extended by the state
                val prevStates: List[State] = nfa.prev(state)
                val prevCandidates: List[SeqAggregateResult] =
                    prevStates.flatMap { s => stateMap.get(s) }

                // the longest running partial match
                // that can be extended through the state
                if( prevCandidates.isEmpty ) None else {
                    val winner: SeqAggregateResult =
                        prevCandidates.minBy { a => a.index }
                    val stateLabel: Label = nfa.stateLabel(state)

                    Some(state -> winner.update(evaluator, row, stateLabel))
                }
            }
        }

        new PartialMatches(
            nfa, aggregates, nextStateMap, index + 1L, Some(row), isLastRow
        )
    }

    def resultOpt: Option[SeqAggregateResult] = {
        val candidates: List[SeqAggregateResult] =
            nfa.finishStates.flatMap { s => stateMap.get(s) }

        if( candidates.isEmpty || (nfa.isAnchoredEnd && !isEnd) ) None else {
            val winner: SeqAggregateResult = candidates.minBy { a => a.index }
            Some(winner)
        }
    }
}

object PartialMatches {
    def apply(
        nfa: AnchoredNfa,
        aggregates: SeqAggregateRows,
        isEnd: Boolean
    ): PartialMatches = {
        val initStateMap: Map[State, SeqAggregateResult] =
            Map(nfa.startState -> SeqAggregateResult(0L, aggregates))

        new PartialMatches(nfa, aggregates, initStateMap, 1L, None, isEnd)
    }
}
