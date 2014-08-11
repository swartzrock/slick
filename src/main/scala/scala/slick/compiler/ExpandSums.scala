package scala.slick.compiler

import scala.slick.SlickException
import scala.slick.ast._
import Util._
import TypeUtil._

/** Expand sum types and their catamorphisms to equivalent product type operations. */
class ExpandSums extends Phase {
  val name = "expandSums"

  def apply(state: CompilerState) = state.map { n => ClientSideOp.mapServerSide(n) { tree =>
    tr(tree)
  }}

  def tr(n: Node): Node = n.nodeMapChildren(tr, keepType = true) match {
    // OptionFold on non-nested Option of single column
    case OptionFold(from :@ Type.Structural(OptionType(Type.Structural(tpe))), ifEmpty, map, gen) if tpe.children.isEmpty =>
      val pred = Library.==.typed[Boolean](from, LiteralNode(null))
      val n2 = (ifEmpty, map) match {
        case (LiteralNode(true), LiteralNode(false)) =>
          pred
        case (LiteralNode(false), LiteralNode(true)) =>
          Library.Not.typed[Boolean](pred)
        case _ =>
          val ifDefined = map.replace({
            case Ref(s) :@ tpe if s == gen => Library.SilentCast.typed(tpe, from)
          }, keepType = true)
          ConditionalExpr(Vector(IfThen(pred, ifEmpty)), ifDefined)
      }
      n2.nodeWithComputedType()
    case n => n
  }
}
