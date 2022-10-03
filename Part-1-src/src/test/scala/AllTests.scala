package test

import org.scalatest._
import funsuite._

import test.distributed._
import test.predict._

class AllTests extends Sequential(
  new test.predict.BaselineTests,
  new test.distributed.DistributedBaselineTests,
  new test.predict.PersonalizedTests,
  new test.predict.kNNTests,
  new test.recommend.RecommenderTests
)

