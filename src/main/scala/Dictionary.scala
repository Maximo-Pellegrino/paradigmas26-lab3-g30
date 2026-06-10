object Dictionary {

  /**
   * Load entities from a dictionary file and create instances of the specified type.
   * Prints a warning if the file is missing or unreadable.
   * @param filePath  path to dictionary file
   * @param entityType type label ("Person", "University", etc.)
   * @param entitiesDir base directory (used only for the warning message)
   * @return list of entities; empty list if the file could not be loaded
   */
  def loadFromFile(filePath: String, entityType: String, entitiesDir: String): List[NamedEntity] = {
    FileIO.readDictionaryFile(filePath) match {
      case None =>
        println(s"Warning: Could not load $filePath")
        List()
      case Some(lines) =>
        lines.map { name =>
          entityType match {
            case "Person"              => new Person(name)
            case "Organization"        => new Organization(name)
            case "University"          => new University(name)
            case "Place"               => new Place(name)
            case "Technology"          => new Technology(name)
            case "ProgrammingLanguage" => new ProgrammingLanguage(name)
          }
        }
    }
  }

  /**
   * Load all dictionary files and combine into a single list.
   * Aborts (prints error and returns empty) if the entities directory does not exist.
   * @param entitiesDir path to directory containing entity files
   * @return combined list of all entities from all successfully loaded dictionaries
   */
  def loadAll(entitiesDir: String): List[NamedEntity] = {
    val dataDir = new java.io.File(entitiesDir)
    if (!dataDir.exists() || !dataDir.isDirectory) {
      println(s"Error: entities directory '$entitiesDir' not found")
      return List()
    }

    val people        = loadFromFile(s"$entitiesDir/people.txt",        "Person",              entitiesDir)
    val universities  = loadFromFile(s"$entitiesDir/universities.txt",  "University",          entitiesDir)
    val languages     = loadFromFile(s"$entitiesDir/languages.txt",     "ProgrammingLanguage", entitiesDir)
    val organizations = loadFromFile(s"$entitiesDir/organizations.txt", "Organization",        entitiesDir)
    val places        = loadFromFile(s"$entitiesDir/places.txt",        "Place",               entitiesDir)

    people ::: universities ::: languages ::: organizations ::: places
  }
}