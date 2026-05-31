import scopt.OParser

case class CommandLineArgs(
  subscriptionFile: String = "data/valid_subscriptions.json",
  entitiesDir: String = "data/valid_entities",
  topK: Int = 10
)

object CommandLineArgs {
  /**
   * Parse command-line arguments using scopt.
   * Supports: --subscription-file, --entities-dir, --top-k
   * All parameters are optional with defaults.
   * @param args command-line arguments
   * @return Option[CommandLineArgs] with parsed arguments, None if parsing fails
   */
  def parse(args: Array[String]): Option[CommandLineArgs] = {
    val builder = OParser.builder[CommandLineArgs]
    val parser = {
      import builder._
      OParser.sequence(
        programName("reddit-ner-scala"),
        head("reddit-ner-scala", "0.1.0"),
        opt[String]("subscription-file")
          .valueName("<file>")
          .action((x, c) => c.copy(subscriptionFile = x))
          .text("path to subscriptions JSON file (default: data/valid_subscriptions.json)"),
        opt[String]("entities-dir")
          .valueName("<dir>")
          .action((x, c) => c.copy(entitiesDir = x))
          .text("path to entities directory (default: data/valid_entities)"),
        opt[Int]("top-k")
          .valueName("<n>")
          .action((x, c) => c.copy(topK = x))
          .validate(x => if (x > 0) success else failure("--top-k must be positive"))
          .text("number of top entities to display (default: 10)")
      )
    }

    OParser.parse(parser, args, CommandLineArgs())
  }
}
