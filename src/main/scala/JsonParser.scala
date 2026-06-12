import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonParser {

  /**
   * Parse Reddit JSON feed and extract posts.
   * @param jsonContent    JSON string from Reddit API
   * @param subscription   the Subscription being parsed (used in warning messages)
   * @return list of posts, empty list if parsing fails
   */
  def parsePosts(jsonContent: String, subscription: Subscription): List[Either[String, Post]] = {
    try {
      implicit val formats: Formats = DefaultFormats

      val json     = parse(jsonContent)
      val children = (json \ "data" \ "children").extract[List[JValue]]

      children.flatMap { child =>
        try {
          val data     = child \ "data"
          val title    = (data \ "title").extract[String]
          val selftext = (data \ "selftext").extract[String]
          Right(Post(title, selftext))
        } catch {
          case _: Exception =>
            println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
            Left("Post fallido")
        }
      }
    } catch {
      case _: Exception =>
        println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
        Left("Post fallido")
    }
  }
}