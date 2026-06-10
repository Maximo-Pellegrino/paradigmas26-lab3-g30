import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  /**
   * Read subscriptions from JSON file.
   * Prints specific error messages for missing file, invalid JSON, and malformed entries.
   * @param filePath path to subscriptions file
   * @return list of valid Subscription objects; exits the program on fatal errors
   */
  def readSubscriptions(filePath: String): List[Subscription] = {
    implicit val formats: Formats = DefaultFormats

    val content = try {
      val source = Source.fromFile(filePath)
      val c = source.mkString
      source.close()
      c
    } catch {
      case _: Exception =>
        println(s"Error: Could not load $filePath - file not found")
        return List() //main se encarga de frenar el programa.
    }

    val rawList: List[Map[String, Any]] = try {
      val json = parse(content)
      json.extract[List[Map[String, Any]]]
    } catch {
      case _: Exception =>
        println(s"Error: Could not load $filePath - invalid JSON format")
        return List() //main se encarga de frenar el programa.
    }

    rawList.flatMap { sub =>
      val nameOpt = sub.get("name").map(_.toString).filter(_.nonEmpty)
      val urlOpt  = sub.get("url").map(_.toString).filter(_.nonEmpty)
      (nameOpt, urlOpt) match {
        case (Some(name), Some(url)) => Some(Subscription(name, url))
        case _ =>
          println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
          None
      }
    }
  }

  /**
   * Download feed JSON from URL.
   * @param subscription the Subscription to download
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(subscription: Subscription): Option[String] = {
    try {
      val source = Source.fromURL(subscription.url)
      val content = source.mkString
      source.close()
      Some(content)
    } catch {
      case _: Exception =>
        println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
        None
    }
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file missing or unreadable
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    try {
      val source = Source.fromFile(filePath)
      val lines = source.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .filterNot(_.startsWith("#"))
        .toList
      source.close()
      Some(lines)
    } catch {
      case _: Exception => None
    }
  }
}