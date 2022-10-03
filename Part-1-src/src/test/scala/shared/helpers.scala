package tests.shared

package object helpers {

  def within(actual :Double, expected :Double, interval :Double) : Boolean = {
    return actual >= (expected - interval) && actual <= (expected + interval)
  }

  def inRange(actual :Double, lower_bound: Double, higher_bound : Double) : Boolean = {
    return actual >= lower_bound && actual <= higher_bound
  }

}
