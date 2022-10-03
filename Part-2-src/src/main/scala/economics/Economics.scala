import org.rogach.scallop._
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import ujson._

package economics {

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}






  

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")
    val ICC_purchase_price = 38600
    val ICC_renting_price = 20.40
    val ICC_ram = 24*64.0
    val ICC_nb_cores = 2*14.0
    val ICC_cpu_ratio_rpi = 4.0

    def minRentingDays() = Math.ceil(ICC_purchase_price / ICC_renting_price)

    val rpi_ram = 8.0
    val container_renting_ram : Double  = 1.6E-7
    val container_renting_cpu : Double = 1.14E-6
    val days_in_seconds = 86400.0

    val rpi_idle_cost_per_hour = 3 * 0.25 / 1000.0
    val rpi_computing_cost_per_hour = 4 * 0.25 /1000.0
    val rpi_purchase_price = 108.48



    def container_daily_cost() = days_in_seconds*(container_renting_cpu+ 4.0 * rpi_ram * container_renting_ram)
    def four_rpis_idle_cost_per_day() = 4 * rpi_idle_cost_per_hour * 24.0
    def four_rpis_computing_cost_per_day() = 4 * rpi_computing_cost_per_hour * 24.0

    def min_nb_days_idle() = Math.ceil(4*rpi_purchase_price / (container_daily_cost() - four_rpis_idle_cost_per_day()))
    def min_nb_days_computing() = Math.ceil(4*rpi_purchase_price / (container_daily_cost() - four_rpis_computing_cost_per_day()))

    def nb_of_rpi_to_icc() = Math.floor(ICC_purchase_price/rpi_purchase_price)
    def ram_rpi_to_icc() = nb_of_rpi_to_icc() * rpi_ram / ICC_ram
    def cpu_rpi_to_icc() = nb_of_rpi_to_icc() / (ICC_cpu_ratio_rpi *ICC_nb_cores)


    var conf = new Conf(args)

    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }
    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {

        val answers = ujson.Obj(
          "E.1" -> ujson.Obj(
            "MinRentingDays" -> minRentingDays() // Datatype of answer: Double
          ),
          "E.2" -> ujson.Obj(
            "ContainerDailyCost" -> container_daily_cost(),
            "4RPisDailyCostIdle" -> four_rpis_idle_cost_per_day(),
            "4RPisDailyCostComputing" -> four_rpis_computing_cost_per_day(),
            "MinRentingDaysIdleRPiPower" -> min_nb_days_idle(),
            "MinRentingDaysComputingRPiPower" -> min_nb_days_computing() 
          ),
          "E.3" -> ujson.Obj(
            "NbRPisEqBuyingICCM7" -> nb_of_rpi_to_icc(),
            "RatioRAMRPisVsICCM7" -> ram_rpi_to_icc(),
            "RatioComputeRPisVsICCM7" -> cpu_rpi_to_icc()
          )
        )

        val json = write(answers, 4)
        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}

}
