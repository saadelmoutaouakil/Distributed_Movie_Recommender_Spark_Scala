package test.shared

package object helpers {

  def within(actual :Double, expected :Double, interval :Double) : Boolean = {
    return actual >= (expected - interval) && actual <= (expected + interval)
  }


}
