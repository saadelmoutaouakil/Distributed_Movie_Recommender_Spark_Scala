package test

import org.scalatest._
import funsuite._

import test.optimizing._
import test.distributed._

class AllTests extends Sequential(
  new OptimizingTests,
  new ExactTests,
  new ApproximateTests
)

