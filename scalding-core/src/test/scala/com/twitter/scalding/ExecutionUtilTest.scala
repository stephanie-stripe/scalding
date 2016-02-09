package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

class ExecutionUtilTest extends WordSpec with Matchers {
  import ExecutionUtil._

  implicit val tz = DateOps.UTC
  implicit val dp = DateParser.default
  implicit val dateRange = DateRange.parse("2015-01-01", "2015-01-10")

  def run[T](e: Execution[T]) = e.waitFor(Config.default, Local(true))

  def testJob(dr: DateRange) =
    TypedPipe
      .from[Int](Seq(1, 2, 3))
      .toIterableExecution
      .map(_.head)

  def testJobFailure(dr: DateRange) = throw new Exception("failed")

  "ExecutionUtil" should {
    "run multiple jobs" in {
      val days = dateRange.each(Days(1)).toSeq
      val result = runDatesWithParallelism(Days(1))(testJob)
      assert(run(result).get == days.map(d => (d, 1)))
    }

    "run multiple jobs with executions" in {
      val days = dateRange.each(Days(1)).toSeq
      val result = runDateRangeWithParallelism(Days(1))(testJob)
      assert(run(result).get == days.map(d => 1))
    }

    "run multiple jobs with executions and sum results" in {
      val days = dateRange.each(Days(1)).toSeq
      val result = runDateRangeWithParallelismSum(Days(1))(testJob)
      assert(run(result).get == days.map(d => 1).sum)
    }

    "handle failure" in {
      val days = dateRange.each(Days(1)).toSeq
      val result = withParallelism(Seq(Execution.failed(new Exception("failed"))), 1)

      assert(run(result).isFailure)
    }

    "handle an error running parallel" in {
      val executions = Execution.failed(new Exception("failed")) :: 0.to(10).map(i => Execution.from[Int](i)).toList

      val result = withParallelism(executions, 3)

      assert(run(result).isFailure)
    }

    "run in parallel" in {
      val executions = 0.to(10).map(i => Execution.from[Int](i)).toList

      val result = withParallelism(executions, 3)

      assert(run(result).get == 0.to(10).toSeq)
    }

    "block correctly" in {
      var seen = 0
      def updateSeen(idx: Int) {
        assert(seen === idx)
        seen += 1
      }

      val executions = 0.to(10).map{ i =>
        Execution
          .from[Int](i)
          .map{ i => Thread.sleep(10 - i); i }
          .onComplete(t => updateSeen(t.get))
      }.toList.reverse

      val result = withParallelism(executions, 1)

      assert(run(result).get == 0.to(10).reverse)
    }
  }
}
