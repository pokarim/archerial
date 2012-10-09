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

package com.pokarim.pprinter
import scala.annotation.tailrec
import scalaz.Free
import scala.collection.immutable

object ColorUtil{
  def withColor(c:scala.Int,s:String,args: Any*) = {
	if (c < 8) "\033[3%sm%s\033[39m" format(c,s) format(args : _*)
	else "\033[38;5;%sm%s\033[39m" format(c,s) format(args : _*)

  }
  def getLen(s:String) = s.replaceAll(
	"\033\\[[^m]+m",""
  ).length
  assert(
	List("List","Hoge(").forall((s) =>getLen(s) == getLen(withColor(10,s)))
  )
}
import ColorUtil.getLen
object `package`{


  import PPrinter._
import ColorUtil._
  import scala.collection
  trait DOC {
	def toPretty(w:Int):String = pretty(w, this)
	def count(w:Int):Int = layoutStream(best(w,0,this)).length
	def join(xs: Seq[DOC]) = folddoc((_ <> this <> _),xs)

  }
  object DOC{
	import scala.collection.mutable.ArrayBuffer
	implicit def Option2Doc[A <% DOC](x:Option[A]):DOC = x match {
	  case None => TEXT("None")
	  case Some(a) => labeledRBracketWC("Some", List(a:DOC))
	}
	implicit def String2Doc(x:String):DOC = TEXT(withColor(2, "\"%s\"" ,x))
	implicit def Any2Doc(x:Any):DOC = TEXT(x.toString)
	implicit def Pair2Doc[A <% DOC,B <% DOC](x:(A,B)) = x match{
	  case (x, y) => group((x:DOC) <> LINE <> NEST(3, TEXT(
		withColor(builtinColor,"-> ")) <> y))
	}
	//val tupArrow = TEXT(withColor(builtinColor,"-> "))

	implicit def Tup32Doc[A <% DOC,B <% DOC,C <% DOC](x:(A,B,C)) = x match{
	   case (x, y, z) => 
	 	group(
	 	  (x:DOC) <>  NEST(3, LINE <> 
						   TEXT(withColor(builtinColor,"-> "))//tupArrow 
						   <> Pair2Doc((y,z)) ))
	 }
	  
	implicit def Map2Doc[A <% DOC,B <% DOC](xs:immutable.Map[A,B]):DOC =
	  labeledRBracketWC("Map", xs.toSeq,builtinColor)
	implicit def List2Doc[A <% DOC](xs:List[A]):DOC = 
	  labeledRBracketWC("List", xs,builtinColor)
	implicit def Set2Doc[A <% DOC](xs:collection.Set[A]):DOC = 
	  labeledRBracketWC("Set", xs.toSeq,builtinColor)
	implicit def ArrayBuffer2Doc[A <% DOC](xs:ArrayBuffer[A]):DOC = 
	  labeledRBracketWC("ArrayBuffer", xs,builtinColor)
	implicit def Stream2Doc[A <% DOC](xs:Stream[A]):DOC = 
	  labeledRBracketWC("Stream", xs,builtinColor)
	//def builtinColor(s:String) = withColor(75,s)
	val builtinColor = 12//75
	implicit def Seq2Doc[A <% DOC](xs:Seq[A]):DOC = 
 
	  (xs : @unchecked) match {
		case xs : ArrayBuffer[_] => ArrayBuffer2Doc[A](xs.asInstanceOf[ArrayBuffer[A]])
		case xs : List[_] => List2Doc[A](xs.asInstanceOf[List[A]])
		case _ :Stream[_] => Stream2Doc[A](xs.asInstanceOf[Stream[A]])
		case _ => labeledRBracketWC(xs.getClass.getName, xs)
		//case _ => String2Doc(xs.toString)
	  }

  }
  import PPrinter._
  def pprn1[A <% DOC ](x : A): A = {pprn(x);x}
  def pprn1[A <% DOC,B <% DOC ](x : A,y :B): B = {pprn(x,y);y}
  def pprn(xs : DOC*){
	if (dopprn.value){
	  val d = fill(xs) <> LINE
	  print(d.toPretty(90))
	}
  }
  import scala.util.DynamicVariable
  val dopprn = new DynamicVariable[Boolean](true)
}



trait Pretty[A]{
  def toDOC(a: A):DOC
}

trait Pretty2{
  def toDOC:DOC
}

object Pretty{
  import PPrinter._

  def pretty[A](f: A =>DOC):Pretty[A] = new Pretty[A]{
	def toDOC(a: A) = f(a)
  }

  implicit def StringPretty:Pretty[String] = pretty((x) => TEXT("\"%s\"" format x))

}
object PPrinter {

  import Stream._
  import Docs._
  def group (x:DOC):DOC = flatten(x) :<|> x

  def flatten: DOC => DOC = {
	case NIL => NIL
	case d: :<>  => new :<>(flatten(d.l),flatten(d.r))
	case NEST(i,x) => NEST(i,flatten(x))
	case d: TEXT => d
	case LINE => TEXT(" ")
	case d: :<|>  => flatten(d.l)}

  def layout(d:Doc) = layoutStream(d).mkString("")
  def layoutStream : Doc=>Stream[String] = {
	case NilDoc => empty
	case s Text x => s #:: layoutStream(x)
	case i Line x => "\n" #:: (" " * i) #:: layoutStream(x)}
  
  def best(w: Int, k: Int, x: DOC):Doc = be(w,k,(0,x)#::empty)

  def be(w: Int, k: Int, xs: Stream[(Int,DOC)]):Doc = xs match {
   	case Empty => NilDoc
   	case ((i,NIL) #:: z) => be(w,k,z)
   	case ((i, d: :<>)#::z) => be(w,k,(i,d.l) #:: (i,d.r) #:: z )
   	case ((i,NEST(j,x))#::z) => be( w, k,((i+j, x) #:: z))
   	case ((i,TEXT(s))#::z) => s Text be(w, (k + getLen(s)), z)
   	case ((i,LINE)#::z) => i Line be( w, i, z)
   	case ((i,d: :<|>)#::z) => 
   	  better( w, k,be(w, k,(i,d.l) #:: z), be(w,k,(i,d.r) #:: z))
   }
  
  def better(w: Int, k: Int, x: =>Doc, y: =>Doc):Doc = if (fits(w-k,x)) x else y

  def fits(w:Int,x: =>Doc):Boolean = 
	w >= 0 && (x match {
	  case NilDoc => true
	  case s Text x => fits( w - getLen(s),x)
	  case _: Line => true
	})
  def pretty(w:Int, x:DOC) = layout(best (w, 0, x))
  def labeledBracket2(left:String,body:DOC,right:String) = {
	NEST(getLen(left), TEXT(left) <> body <>TEXT(right))
  }
  def labeledBracket(left:String,body:DOC,right:String) = 
	group(NEST(getLen(left), TEXT(left) <> body <>TEXT(right)))

  def labeledBracketWC[A <% DOC](left:String,body:Seq[A],right:String,c:DOC=TEXT(",")) =
	labeledBracket(left, joinWComma(body.map((x:A) => x:DOC), c), right)

  def labeledBracketWC2[A <% DOC](left:String,body:Seq[A],right:String,c:DOC=TEXT(",")) =
	labeledBracket2(left, joinWComma(body.map((x:A) => x:DOC), c), right)

import ColorUtil.withColor
  def labeledRBracketWC[A <% DOC](left:String,body:Seq[A],col:Int=7) =
	labeledBracketWC(withColor(col,left+ "(") , body, 
					 withColor(col,")"),TEXT(withColor(col,",")))

  def labeledRBracketWC2[A <% DOC](left:String,body:Seq[A],col:Int=7) =
	labeledBracketWC2(withColor(col,left+ "(") , body, 
					 withColor(col,")"),TEXT(withColor(col,",")))


  def joinWComma(xs: Seq[DOC],c:DOC=TEXT(",")):DOC = 
	(c <> LINE).join(xs)
  def folddoc(f:(=>DOC,=>DOC) => DOC, xs:Seq[DOC]):DOC = xs match{
	case _ if xs.isEmpty => NIL
	case _ if xs.tail.isEmpty => xs.head
	case _ => f(xs.head,folddoc(f,xs.tail))
  }

  def fill(xs:scala.collection.GenTraversableOnce[DOC]):DOC = _fill(xs.toStream)
  def _fill(xs:Stream[DOC]):DOC = xs match{
	case Empty if xs.isEmpty => NIL
	case x #:: Empty => x
	case x #:: y #:: zs => 
	  (flatten(x) <+> _fill(flatten(y) #:: zs)) :<|> (x </> _fill(y #:: zs))
  }
  
  def spread(xs:Seq[DOC]):DOC = folddoc((_ <+> _), xs)
  def stack(xs:Seq[DOC]):DOC = folddoc((_ </> _), xs)

  
  object NIL extends DOC {
	override def toString = "NIL"
  }
  case class NEST(n:Int, d:DOC) extends DOC
  case class TEXT(s:String) extends DOC
  object LINE extends DOC{
	override def toString = "LINE"
  }
  
  class DOCwrapper(l : =>DOC){
    def <>(r : => DOC) = new :<>(l,r)
	def <+>(r : => DOC) = l <> TEXT(" ") <> r
	def </>(r : => DOC) = l <> LINE <> r
    def :<>(r : => DOC) = new :<>(l,r)
    def :<|>(r : => DOC) = new :<|>(l,r)
  }

  implicit def docWrapper(l: => DOC): DOCwrapper = new DOCwrapper(l)
  
  class :<> (_l: => DOC, _r: =>DOC) extends DOC {
	def l = _l
	def r = _r
  }

  class :<|> (_l: => DOC, _r: =>DOC) extends DOC{
	def l = _l
	def r = _r
  }

}
object Docs {
  trait Doc
  class StringWrapper(s : String){
	def Text(d: =>Doc): Doc = new Text(s,d)
  }
  class IntWrapper(i : Int){
	def Line(d: =>Doc): Doc = new Line(i,d)
  }
  class Text(val s:String, _d: =>Doc) extends Doc{
	def d = _d
  }
  object Text {
	def unapply(t:Text) = Some((t.s,t.d))
  }

  class Line(val i:Int, _d: =>Doc) extends Doc{
	def d = _d
  }
  object Line {
	def unapply(x:Line) = Some((x.i,x.d))
  }

  implicit def stringWrapper(s :String):StringWrapper = new StringWrapper(s)
  implicit def intWrapper(i :Int):IntWrapper = new IntWrapper(i)
  object NilDoc extends Doc {
	override def toString = "Nil"
  }

}
