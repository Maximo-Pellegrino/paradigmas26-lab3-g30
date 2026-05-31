import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonParser {

  /**
   * Parse Reddit JSON feed and extract posts.
   * @param jsonContent JSON string from Reddit API
   * @param subscriptionName name of subscription (for logging)
   * @return list of posts, empty list if parsing fails
   */
  def parsePosts(jsonContent: String, subscriptionName: String): List[Post] = {
    try {
      implicit val formats: Formats = DefaultFormats

      val json = parse(jsonContent)
      val children = (json \ "data" \ "children").extract[List[JValue]]

      children.flatMap { child =>
        val data = child \ "data"
        val title = (data \ "title").extract[String]
        val selftext = (data \ "selftext").extract[String]
        List(Post(title, selftext))
      }
    } catch {
      case _: Exception =>
        println(s"Warning: Failed to parse JSON from '$subscriptionName'")
        List()
    }
  }
}
