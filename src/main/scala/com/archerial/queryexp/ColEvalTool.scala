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

import com.archerial.Column
import com.archerial.Value
import com.archerial.utils._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._
import scala.collection.immutable

object ColEvalTool{

  def eval(colExp:ColExp, vcol:ColExp, values:Seq[Value], getter:RowsGetter,dropNull:Boolean=true ): Seq[Value] = {
	assert(getter.dict.contains((vcol,colExp)))
	val vs = for {x <- values
				  v <- getter.dict((vcol,colExp))(x)
				  if !dropNull || v.nonNull	}
			 yield v
	vs
  }
}

trait TupleExpBase {
  def row2value(row:Row ): Value =
	 throw new Exception("hoge")

  def getSQL(map: TableIdMap):String = {
	throw new Exception("invalid operation")
  }

  def getDependentCol():Stream[ColExp] = 
	(keyExp::valExps).toStream.flatMap(_.getDependentCol())

  def keyExp:QueryExp
  def valExps:List[QueryExp]
}
