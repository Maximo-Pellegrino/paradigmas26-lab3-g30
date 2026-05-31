abstract class NamedEntity(val text: String) {
  def entityType: String

  def describe: String = s"[$entityType] $text"
}

class Person(text: String) extends NamedEntity(text) {
  def entityType: String = "Person"
}

class Organization(text: String) extends NamedEntity(text) {
  def entityType: String = "Organization"
}

class University(text: String) extends Organization(text) {
  override def entityType: String = "University"
}

class Place(text: String) extends NamedEntity(text) {
  def entityType: String = "Place"
}

class Technology(text: String) extends NamedEntity(text) {
  def entityType: String = "Technology"
}

class ProgrammingLanguage(text: String) extends Technology(text) {
  override def entityType: String = "ProgrammingLanguage"
}
