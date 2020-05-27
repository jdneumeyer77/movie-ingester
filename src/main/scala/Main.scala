import java.time.LocalDate
import java.{util => ju}
import OutputModels.ProductionCompanyDetails
import scala.collection.immutable.SortedMap
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.java8._
import better.files._
import File._
import scala.util.Try
import play.api.libs.json._
import java.time.format.DateTimeFormatter

// TODO: Last run output and accept it as input for filtering
object Main extends App {

  val DTYearMonthFormat = DateTimeFormatter.ofPattern("yyyy-MM")

  println(args.length)

  val (inputFile, optionalLastRun) = args.length match {
    case a if a == 2 =>
      (args(0), Option(args(1)).map(LocalDate.parse(_, DTYearMonthFormat)).getOrElse(LocalDate.MIN))
    case a if a == 1 => (args(0), LocalDate.MIN)
    case _ =>
      println(s"Invalid number of args! [lastrun:YYYY-MM]")
      System.exit(666)
      ("", LocalDate.MIN) // so the compiler doesn't complain
  }

  val input = File.apply(inputFile)
  if (!input.exists || !input.isReadable) {
    println(s"Cannot read file: ${input.name}")
    System.exit(666)
  } else {
    println(s"Processing file ${input.name}")
  }

  import IngestModel._
  val reader = input.uri.asCsvReader[MovieRow](
    rfc.withHeader(true)
  )

  if (!optionalLastRun.isEqual(LocalDate.MIN))
    println(s"Filtering everything before ${optionalLastRun}")

  def toKeep(movie: MovieRow): Boolean = {
    movie.release.isAfter(optionalLastRun) &&
    movie.status.equalsIgnoreCase("released") &&
    movie.revenue > 0 // missing, not reported revenue? excludes a lot of movies in the test set.
  }

  // lazy iterator; will not consume entire thing into memory.
  // We coul easily wrap this as Akka Streams Source to introduce buffering and enhacing the data using external means (imbd, internal db, http call, etc.)
  val results = reader
    .collect {
      // Based on the last run or a specified date to filter on, i.e. only collect this months results if after a full run is done.
      // further filter can be done here to speed up the ingestion such as eliminating planned or cancelled movies.
      case Right(movie) if toKeep(movie) => movie
    }
    .foldLeft(OutputModels.DetailCollector()) {
      case (acc, nextRow) => acc.accept(nextRow)
    }

  results.stats()

  val outputDir = file"./output/"
  results.output(outputDir)
}

object IngestModel {
  import collection.immutable.Map

  implicit val movieRowDecoder: HeaderDecoder[MovieRow] =
    HeaderDecoder.decoder(
      "id",
      "genres",
      "production_companies",
      "release_date",
      "budget",
      "revenue",
      "popularity",
      "status"
    )(MovieRow.apply _)

  case class MovieRow(
      movieId: String,
      genreIdsJson: String,
      productionCompanyIdsJson: String,
      release: LocalDate,
      budget: Long,
      revenue: Long,
      avgPopulatarity: Double,
      status: String
  ) {
    val profit = revenue - budget

    // seriously who embeds json  in a csv file??!
    // delay parsing the json as it's expensive and the row may be filtered out.
    lazy val genreIds: Set[String] = Try(
      (Json.parse(genreIdsJson.replace("'", "\"")) \\ "id").map(_.as[Int].toString).toSet
    ).getOrElse {
      Set.empty
    }

    lazy val productionCompanyIds: Set[String] = Try(
      (Json.parse(productionCompanyIdsJson.replace("'", "\"")) \\ "id")
        .map(_.as[Int].toString)
        .toSet
    ).getOrElse(Set.empty)

    def toPrudctionCompanies: Iterable[OutputModels.ProductionCompanyDetails] = {
      val metadata = OutputModels.ProductionCompanyMetadata(Set(movieId), genreIds)
      productionCompanyIds.map(id =>
        OutputModels.ProductionCompanyDetails(
          id,
          release,
          budget,
          profit,
          revenue,
          avgPopulatarity,
          metadata
        )
      )
    }

    def toGenresDetails: Iterable[OutputModels.GenreDetails] = {
      val metadata = OutputModels.GenreDetailsMetadata(Set(movieId), genreIds)
      genreIds.map(id =>
        OutputModels
          .GenreDetails(id, release, budget, profit, revenue, avgPopulatarity, metadata)
      )
    }
  }
}

object OutputModels {

  sealed trait Id[A] {
    def id: String
    def date: LocalDate
    def sum(a: A): A
  }
  case class ProductionCompanyMetadata(movieIds: Set[String], genreIds: Set[String])
  case class ProductionCompanyDetails(
      id: String,
      date: LocalDate,
      budget: Long,
      profit: Long,
      revenue: Long,
      avgPopulatarity: Double,
      metadata: ProductionCompanyMetadata
  ) extends Id[ProductionCompanyDetails] {
    def sum(a: ProductionCompanyDetails): ProductionCompanyDetails = {
      a.copy(
        profit = a.profit + this.profit,
        budget = a.budget + this.budget,
        revenue = a.revenue + this.revenue,
        avgPopulatarity = (a.avgPopulatarity + this.avgPopulatarity) / 2,
        metadata = metadata.copy(
          movieIds = metadata.movieIds ++ a.metadata.movieIds,
          genreIds = metadata.genreIds ++ a.metadata.genreIds
        )
      )
    }
  }

  implicit val prodCompanyMetadataJsonEnc = Json.writes[ProductionCompanyMetadata]
  implicit val prodCompanyJsonEnc = Json.writes[ProductionCompanyDetails]

  case class GenreDetailsMetadata(movieIds: Set[String], productionCompanyIds: Set[String])
  case class GenreDetails(
      id: String,
      date: LocalDate,
      budget: Long,
      profit: Long,
      revenue: Long,
      avgPopulatarity: Double,
      metadata: GenreDetailsMetadata
  ) extends Id[GenreDetails] {
    def sum(a: GenreDetails): GenreDetails = {
      a.copy(
        profit = a.profit + this.profit,
        budget = a.budget + this.budget,
        revenue = a.revenue + this.revenue,
        avgPopulatarity = (a.avgPopulatarity + this.avgPopulatarity) / 2,
        metadata = metadata.copy(
          movieIds = this.metadata.movieIds ++ a.metadata.movieIds,
          productionCompanyIds = metadata.productionCompanyIds ++ a.metadata.productionCompanyIds
        )
      )
    }
  }

  implicit val genreMetadataJsonEnc = Json.writes[GenreDetailsMetadata]
  implicit val genreJsonEnc = Json.writes[GenreDetails]

  type BucketedYearMap[A] = SortedMap[Int, Array[Map[String, A]]]
  type BucketedYearFlattened[A] = SortedMap[Int, Array[Iterable[A]]]

  // Year -> Map(ProductionCompanyId -> List of Details)
  type ProductCompanyDetailsMap = BucketedYearMap[ProductionCompanyDetails]
  private def newProductDetailsMap: ProductCompanyDetailsMap =
    SortedMap.empty[Int, Array[Map[String, ProductionCompanyDetails]]]

  // Year -> Map(ProductionCompanyId -> List of Details)
  type GenreDetailsMap = BucketedYearMap[GenreDetails]
  private def newGenreDetailsMap: GenreDetailsMap =
    SortedMap.empty[Int, Array[Map[String, GenreDetails]]]

  implicit final class YearMapExt[A <: OutputModels.Id[A]](val map: BucketedYearMap[A])
      extends AnyVal {
    def addDetail(detail: A): BucketedYearMap[A] = {
      val year = detail.date.getYear
      val months = map.getOrElse(year, Array.fill(12)(Map.empty[String, A]))
      val monthIdx = detail.date.getMonthValue - 1
      val month = months(monthIdx)
      val updatedMonth =
        month.updated(detail.id, month.get(detail.id).map(_.sum(detail)).getOrElse(detail))
      map.updated(year, months.updated(monthIdx, updatedMonth))
    }

    def flat: BucketedYearFlattened[A] = {
      map.map {
        case (k, v) =>
          k -> v.filter(_.nonEmpty).map(x => x.values)
      }
    }
  }

  // TODO: accepts movieRow, but split out collectors.
  case class DetailCollector(
      moviesSeen: Int = 0,
      genresDetails: GenreDetailsMap = newGenreDetailsMap,
      prodCompanyDetails: ProductCompanyDetailsMap = newProductDetailsMap
  ) {
    def accept(row: IngestModel.MovieRow): DetailCollector = {

      val updatedGenreDetails = row.toGenresDetails.foldLeft(genresDetails) {
        case (acc, next) => acc.addDetail(next)
      }

      val updatedProdCompanyDetails = row.toPrudctionCompanies.foldLeft(prodCompanyDetails) {
        case (acc, next) => acc.addDetail(next)
      }

      this.copy(
        moviesSeen = this.moviesSeen + 1,
        genresDetails = updatedGenreDetails,
        prodCompanyDetails = updatedProdCompanyDetails
      )
    }

    def stats(): Unit = {
      println(s"Total movies seen: $moviesSeen")
      // println(s"genres years: ${genresDetails}")
    }

    // For simplicity just output in json. It could easily be in Parquet, SQLite db, etc.
    // Parquet might be the best if we add a lot more columns orthe next step does further analytics, or want to append a month.
    // TODO: format dates YYYY-MM
    def outputDetailsJson[A: Writes](outputPath: File, details: BucketedYearFlattened[A]) = {
      details.foreach {
        case (year, months) =>
          val yearDir = (outputPath / year.toString)

          months.zipWithIndex.foreach {
            case (v, idx) =>
              val file = yearDir / s"$year-${(idx + 1)}.json"

              //  println(s"writing file ${file.name}")
              val asJson = Json.arr(v.map(Json.toJson[A]))
              file.createFileIfNotExists(true).writeByteArray(Json.toBytes(asJson))
          }
      }
    }

    def output(outputPath: File) = {

      val out = outputPath

      val genresOut = out / "genres"
      val prodCompanyOut = out / "productionCompanies"

      outputDetailsJson(genresOut, genresDetails.flat)

      outputDetailsJson(prodCompanyOut, prodCompanyDetails.flat)
    }
  }
}
