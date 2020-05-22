object Main extends App {
  val (inputFile, OptionalLastRun) = args.length match {
    case a if a == 2 => (args(0), Option(args(1)))
    case a if a == 1 => (args(0), None)
    case _ => println(s"Invalid number of args! inputFile [lastrun.json]")
  } 
}


object IngestModel {

}

object OutputModels {

  case class ProductionCompanyMetadata(movieIds: Set[String], genreIds: Set[String])
  case class ProductionCompanyDetails(id: String, name: String, date: Instance, budget: Long, profit: Long, avgPopulatarity: Double, metadata: ProductionCompanyMetadata) {
    val profit = revenue - budget
  }


}