// porque extends serializable?
// Porque los objetos de tipo NamedEntity viajan por la red entre el driver y
// los workers de Spark.Porque los objetos de tipo NamedEntity viajan por la
// red entre el driver y los workers de Spark.
// Específicamente pasa en dos momentos del programa:
//     1. El broadcast del diccionario: 
//     val dictBroadcast = sc.broadcast(dictionary) // List[NamedEntity] viaja a todos los workers
//     2. El RDD de entidades detectadas:
//     val allEntitiesRDD = filteredPostsRDD.flatMap { post =>
//       Analyzer.detectEntities(...) // devuelve List[NamedEntity], viaja entre workers
//     }
// Para mandar un objeto por la red, Spark necesita convertirlo a bytes
// (serializar) y después reconstruirlo del otro lado (deserializar). Si la
// clase no implementa Serializable, Spark lanza una excepción en runtime.
abstract class NamedEntity(val text: String) extends Serializable {
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