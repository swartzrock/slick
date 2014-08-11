package com.typesafe.slick.testkit.tests

import org.junit.Assert._
import com.typesafe.slick.testkit.util.{RelationalTestDB, TestkitTest}

class NestingTest extends TestkitTest[RelationalTestDB] {
  import tdb.profile.simple._

  override val reuseInstance = true

  def testNestedTuples {
    import TupleMethods._

    class T(tag: Tag) extends Table[(Int, String, String)](tag, "T") {
      def a = column[Int]("A")
      def b = column[String]("B")
      def c = column[String]("C")
      def * = (a, b, c)
    }
    val ts = TableQuery[T]

    ts.ddl.create
    ts ++= Seq((1, "1", "a"), (2, "2", "b"), (3, "3", "c"))

    val res1 = List(
      (1, "1", "a", 5), (2, "2", "a", 5), (3, "3", "a", 5),
      (1, "1", "b", 5), (2, "2", "b", 5), (3, "3", "b", 5),
      (1, "1", "c", 5), (2, "2", "c", 5), (3, "3", "c", 5)
    )
    val res1b = res1.map { case (a, b, c, d) => ((a, b), (c, d)) }

    val q1a = (for {
      (a, b) <- ts.map(t => (t.a, t.b))
      c <- ts.map(t => t.c)
    } yield a ~ b ~ c ~ 5).sortBy(t => t._3 ~ t._1)
    assertEquals(res1, q1a.run)

    val q1c = (for {
      a ~ b <- ts.map(t => (t.a, t.b))
      c <- ts.map(t => t.c)
    } yield (a, b, c, LiteralColumn(5))).sortBy(t => t._3 ~ t._1)
    assertEquals(res1, q1c.run)

    val q1d = (for {
      (a, b) <- ts.map(t => (t.a, t.b))
      c <- ts.map(t => t.c)
    } yield ((a, b), (c, 5))).sortBy(t => t._2._1 ~ t._1._1)
    assertEquals(res1b, q1d.run)

    val res2 = Set((1, "1", 8), (2, "2", 10))

    val q2a = for {
      a ~ b ~ c <- ts.filter(_.a === 1).map(t => t.a ~ t.b ~ 4) unionAll ts.filter(_.a === 2).map(t => t.a ~ t.b ~ 5)
    } yield a ~ b ~ (c*2)
    assertEquals(res2, q2a.run.toSet)

    val q2b = for {
      (a, b, c) <- ts.filter(_.a === 1).map(t => (t.a, t.b, LiteralColumn(4))) unionAll ts.filter(_.a === 2).map(t => (t.a, t.b, LiteralColumn(5)))
    } yield a ~ b ~ (c*2)
    assertEquals(res2, q2b.run.toSet)

    val q2c = for {
      (a, b, c) <- ts.filter(_.a === 1).map(t => (t.a, t.b, 4)) unionAll ts.filter(_.a === 2).map(t => (t.a, t.b, 5))
    } yield a ~ b ~ (c*2)
    assertEquals(res2, q2c.run.toSet)
  }

  def testNestedOptions {
    class X(tag: Tag) extends Table[(Int, String, Option[String])](tag, "X_OPT") {
      def a = column[Int]("A")
      def b = column[String]("B")
      def c = column[Option[String]]("C")
      def * = (a, b, c)
    }
    val xs = TableQuery[X]

    xs.ddl.create
    xs ++= Seq((1, "1", Some("a")), (2, "2", Some("b")), (3, "3", None))

    // Construct all kinds of Option Shapes
    implicitly[Shape[_, Rep[Int], _, _]]
    implicitly[Shape[_, Rep[Option[Int]], _, _]]
    implicitly[Shape[_, Rep[Option[Option[Int]]], _, _]]
    implicitly[Shape[_, Rep[Option[(Rep[Int], Rep[String])]], _, _]]
    implicitly[Shape[_, Rep[Option[X]], _, _]]

    // Construct all different kinds of Options
    def q1 = xs.map(t => Rep.Some(t))
    def q1a2 = xs.map(t => Rep.Some(Rep.Some(t)))
    def q2 = xs.map(t => Rep.Some(t.a))
    def q2a2 = xs.map(t => Rep.Some(Rep.Some(t.a)))
    def q3 = xs.map(t => t.c)
    def q4 = xs.map(t => Rep.Some(t.c))
    def q5 = xs.map(t => (t.c, Rep.Some(t.b)))
    def q1t: Query[Rep[Option[X]], _, Seq] = q1
    def q1a2t: Query[Rep[Option[Option[X]]], _, Seq] = q1a2
    def q2t: Query[Rep[Option[Int]], _, Seq] = q2
    def q2a2t: Query[Rep[Option[Option[Int]]], _, Seq] = q2a2
    def q3t: Query[Rep[Option[String]], _, Seq] = q3
    def q4t: Query[Rep[Option[Option[String]]], _, Seq] = q4
    def q5t: Query[(Rep[Option[String]], Rep[Option[String]]), _, Seq] = q5

    // Get plain values out
    def q1b = q1.map(_.map(x => (x.a, x.b, x.c)).getOrElse((0, "", None: Option[String])))
    def q2b = q2.map(_.get)
    def q3b = q3.map(_.get)
    def q4b = q4.map(_.getOrElse(None: Option[String]))
    def q1bt: Query[(Rep[Int], Rep[String], Rep[Option[String]]), _, Seq] = q1b
    def q2bt: Query[Rep[Int], _, Seq] = q2b
    def q3bt: Query[Rep[String], _, Seq] = q3b
    def q4bt: Query[Rep[Option[String]], _, Seq] = q4b

    // Unpack result types
    def r1: Seq[Option[(Int, String, Option[String])]] = q1.run
    def r2: Seq[Option[Int]] = q2.run
    def r3: Seq[Option[String]] = q3.run
    def r2b: Seq[Int] = q2b.run
    def r3b: Seq[String] = q3b.run

    // Perform Option-mapped operations
    def q2c = q2.map(io => io + 42)
    def q3c = q3.map(so => so ++ "x")

    // Use Option.map
    def q1d = q1.map(_.map(_.a))
    def q1d2 = q1.map(_.map(x => (x.a, x.b, x.c)))
    def q2d = q2.map { io: Rep[Option[Int]] =>
      io.map { i: Rep[Int] =>
        i + 1
      }
    }
    def q3d = q3.map(_.map(s => (s, s, 1)))
    def q4d = q4.map(_.map(_.get))
    def q1dt: Query[Rep[Option[Int]], _, Seq] = q1d
    def q1d2t: Query[Rep[Option[(Rep[Int], Rep[String], Rep[Option[String]])]], _, Seq] = q1d2
    def q2dt: Query[Rep[Option[Int]], _, Seq] = q2d
    def q3dt: Query[Rep[Option[(Rep[String], Rep[String], ConstColumn[Int])]], _, Seq] = q3d
    def q4dt: Query[Rep[Option[String]], _, Seq] = q4d

    // Use Option.flatMap
    def q1e1 = q1.map { to => to.flatMap { t => Rep.Some(t.b) }}
    def q1e2 = q1.map { to => to.flatMap { t => t.c }}
    def q1e3 = q1.map(to => Rep.Some(to)).map(_.flatMap(identity))
    def q2e = q2.map { io => io.flatMap { i => Rep.Some(i) }}
    def q1e1t: Query[Rep[Option[String]], _, Seq] = q1e1
    def q1e2t: Query[Rep[Option[String]], _, Seq] = q1e2
    def q2et: Query[Rep[Option[Int]], _, Seq] = q2e

    // Use Option.flatten
    def q1f1 = q1.map { to => Rep.Some(to) }
    def q1f2 = q1.map { to => Rep.Some(to).flatten }
    def q1f3 = q1.map { to => Rep.Some(to) }.map(_.flatten)
    def q2f1 = q2.map { io => Rep.Some(io) }
    def q2f2 = q2.map { io => Rep.Some(io).flatten }
    def q2f3 = q2.map { io => Rep.Some(io) }.map(_.flatten)
    def q1f1t: Query[Rep[Option[Option[X]]], _, Seq] = q1f1
    def q1f2t: Query[Rep[Option[X]], _, Seq] = q1f2
    def q1f3t: Query[Rep[Option[X]], _, Seq] = q1f3
    def q2f1t: Query[Rep[Option[Option[Int]]], _, Seq] = q2f1
    def q2f2t: Query[Rep[Option[Int]], _, Seq] = q2f2
    def q2f3t: Query[Rep[Option[Int]], _, Seq] = q2f3
  }
}
