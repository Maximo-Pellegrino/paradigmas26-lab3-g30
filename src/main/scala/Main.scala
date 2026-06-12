import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {

    // 1. Parsear argumentos de línea de comandos
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None         => return
    }

    // 2. Crear SparkSession (modo local)
    // SparkSession es el punto de entrada a Spark. Se construye con un patrón
    // builder, es decir, se encadenan opciones antes de crearlo
    val spark = SparkSession.builder() 
      .appName("RedditNER") // el nombre que aparece en los logs y en la UI de Spark
      .master("local[*]") //  dónde corre Spark
      .getOrCreate() //si ya existe una sesión activa la reutiliza, si no crea una nueva

    // Por defecto Spark imprime muchísimos logs. Con WARN solo muestra
    // advertencias y errores, así la salida del programa es legible.
    spark.sparkContext.setLogLevel("WARN")

    // SparkSession es la API moderna (para DataFrames, SQL). SparkContext es
    // la API de más bajo nivel, necesaria para trabajar con RDDs como hace
    // este programa. Se extrae del spark para usarlo más cómodamente después,
    // por ejemplo en sc.parallelize(...) o sc.longAccumulator(...).
    val sc: SparkContext = spark.sparkContext

    // 3. Cargar suscripciones (driver, I/O secuencial)
    // readSubscriptions ya imprime errores y termina en fallos fatales.
    val subscriptions: List[Subscription] =
      FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      return
    }

    // 4. Distribuir suscripciones y descargar feeds en paralelo
    //
    // Cada elemento del RDD es una Subscription.
    // El flatMap:
    //   a) intenta descargar el feed      -> registra feedSuccess / feedFailed
    //   b) intenta parsear los posts      -> registra postsSuccess / postsFailed
    //   c) filtra posts vacíos inline     -> registra postsFiltered
    //   d) devuelve un iterador de objetos Post válidos (no vacíos)
    //
    // Todos los errores se manejan *dentro* del lambda para que una
    // suscripción fallida no cancele el resto del job de Spark.
    // Por qué se hace así y no cada worker lee el archivo directamente:
    // El archivo de suscripciones es pequeño y está en la máquina del driver.
    // Sería más complejo hacer que cada worker lo lea, porque:
    //     - Tendrían que saber dónde está el archivo
    //     - Tendrían que tener acceso de red a esa máquina
    // Leerían el mismo archivo múltiples veces
    // Es más simple que el driver lo lea una vez y reparta el resultado. Para
    // archivos grandes en producción se usaría HDFS o S3, donde todos los
    // workers tienen acceso directo.
    val subsRDD = sc.parallelize(subscriptions)

    // Acumuladores para conteo por partición (seguros entre workers)
    val feedSuccessAcc  = sc.longAccumulator("feedsSuccess")
    val feedFailedAcc   = sc.longAccumulator("feedsFailed")
    val postSuccessAcc  = sc.longAccumulator("postsSuccess")
    val postFailedAcc   = sc.longAccumulator("postsFailed")
    val totalCharsAcc   = sc.longAccumulator("totalChars")

    // RDD[Post] — ya filtrado (título y selftext no vacíos)
    val filteredPostsRDD = subsRDD.flatMap { subscription =>
      val feedOpt: Option[String] = FileIO.downloadFeed(subscription)

      if (feedOpt.isEmpty) {
        feedFailedAcc.add(1)
        Iterator.empty
      } else {
        feedSuccessAcc.add(1)

        val results: List[Either[String, Post]] =
          JsonParser.parsePosts(feedOpt.get, subscription)

        val rawPosts   = results.collect { case Right(post) => post }
        val failedCount = results.count(_.isLeft)

        if (rawPosts.nonEmpty) postSuccessAcc.add(rawPosts.length)
        if (failedCount > 0)   postFailedAcc.add(failedCount) 

        val valid = rawPosts.filter { post =>
          post.title.nonEmpty &&
          post.selftext.trim.nonEmpty
        }

        val chars = valid.map(p => p.title.length + p.selftext.length).sum
        totalCharsAcc.add(chars)

        valid.iterator
      }
    }.cache() // evitamos recalcular todo el pipeline

    //Ejecutamos la descarga y filtrado para llenar los acumuladores antes de imprimir estadísticas.
    val totalValidPosts = filteredPostsRDD.count()

    // 7. Guardia: ningún post válido
    if (totalValidPosts == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }

    // 8. Cargar diccionario en el driver y hacer broadcast
    // loadAll ya imprimió el error de directorio no encontrado si corresponde.
    val dictionary: List[NamedEntity] = Dictionary.loadAll(cmdArgs.entitiesDir)
    val dictBroadcast = sc.broadcast(dictionary)

    // 9. Detectar entidades (distribuido)
    val allEntitiesRDD = filteredPostsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictBroadcast.value)
    }.cache() // con el cache evitamos volver a detectar entidades en los siguientes pasos.

    // 10. Contar entidades (reduceByKey distribuido)
    // reduceByKey: Es un groupBy + op en un solo paso distribuido. Toma pares
    // (clave, valor) y combina todos los valores que tienen la misma clave
    // usando una función, en este caso _ + _ (suma).
    // Ver cómo spark lo distribuye en informe.md
    val entityCountsRDD = allEntitiesRDD
      .map(e => ((e.entityType, e.text), 1))
      .reduceByKey(_ + _)

    val typeCountsRDD = allEntitiesRDD
      .map(e => (e.entityType, 1))
      .reduceByKey(_ + _)

    // 5. Imprimir estadísticas de procesamiento
    val avgChars: Long = totalCharsAcc.value / totalValidPosts
    // Nota: la re-descarga de arriba sería muy costosa en producción;
    // para mayor precisión nos apoyamos en los acumuladores del primer pasaje.
    val stats = Map(
      "feedsSuccess"  -> feedSuccessAcc.value.toInt,
      "feedsFailed"   -> feedFailedAcc.value.toInt,
      "postsSuccess"  -> postSuccessAcc.value.toInt,
      "postsFailed"   -> postFailedAcc.value.toInt,
      "postsFiltered" -> totalValidPosts.toInt,
      "avgChars"      -> avgChars.toInt
    )

    println(Formatters.formatProcessingStats(stats))
    println()

    // Colectar en el driver para formatear (conjuntos de resultados pequeños)
    // collect() trae todos los datos de los workers al driver, convirtiendo el
    // RDD en un array normal de Scala.
    val entityCounts: Map[(String, String), Int] =
      entityCountsRDD.collect().toMap

    val typeCountsMap: Map[String, Int] =
      typeCountsRDD.collect().toMap

    val totalEntities = allEntitiesRDD.count()
    val typeStats     = typeCountsMap + ("total" -> totalEntities.toInt)

    // 11. Imprimir estadísticas de entidades
    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    // Liberamos cache para liberar memoria.
    filteredPostsRDD.unpersist()
    allEntitiesRDD.unpersist()

    spark.stop()
  }
}