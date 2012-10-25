/**
 * Copyright 2012 Mikio Hokari
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  */

package com.archerial.queryexp
import scala.collection.immutable
import com.archerial._
import com.archerial.utils.implicits._
import com.archerial.utils._

case class TreeColInfo(col2table:Rel[ColExp,TableExp],trees:Seq[TableTree]){
  val root:TableTree = trees(0)
  val table_tree = 
	Rel.gen(trees)(UnitTable #:: _.tableExps).inverse
  val table_tree_NoUnit = 
	Rel.gen(trees)(_.tableExps).inverse
  val tree_col:Rel[TableTree,ColExp] = 
	(col2table ++ table_tree).inverse
  val table2col = col2table.inverse
}



