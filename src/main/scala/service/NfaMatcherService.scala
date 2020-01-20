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

package com.scleradb.plugin.analytics.sequence.matcher.service

import com.scleradb.util.automata.nfa.AnchoredNfa
import com.scleradb.plugin.analytics.sequence.matcher.NfaMatcher

import com.scleradb.sql.expr.ColRef

import com.scleradb.analytics.sequence.matcher.aggregate.SeqAggregateRowsSpec
import com.scleradb.analytics.sequence.matcher.service.SequenceMatcherService

class NfaMatcherService extends SequenceMatcherService {
    override val id: String = "NFAMATCHER"

    override def createMatcher(
        anchoredNfa: AnchoredNfa,
        aggregateSpec: SeqAggregateRowsSpec,
        partitionCols: List[ColRef]
    ): NfaMatcher = new NfaMatcher(anchoredNfa, aggregateSpec, partitionCols)
}
