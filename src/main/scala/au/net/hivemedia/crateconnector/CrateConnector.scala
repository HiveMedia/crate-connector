package au.net.hivemedia.crateconnector

import java.io.IOException
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import io.crate.client.CrateClient

/**
 * CrateConnector provides methods
 * to make the creation and deletion of,
 * and retrieval of data from tables
 * based on case classes as easy as possible
 *
 * @author Liam Haworth
 * @version 1.0
 */
object CrateConnector {

  /**
   * Generates and executes a SQL statement designed to
   * build a table for the object in the selected schema
   *
   * @param schema The schema to create the table under
   * @throws java.io.IOException Thrown when the method fails to successfully execute the sql statement
   * @return Boolean Returns true if a new table was created
   */
  @throws(classOf[IOException])
  def create(schema: String, objClass: Class[_ <: CrateObject], primaryKey: String = "")(implicit crateClient: CrateClient): Boolean = {
    if(crateClient == null)
      throw new IOException("Requires implicit object crateClient to be initialized")

    if(exists(schema, objClass)(crateClient))
      return false

    val tableName = objClass.getSimpleName.toLowerCase
    var tableColumns = Map.empty[String, String]

    objClass.getDeclaredFields.foreach { f =>
      if(!f.getName.startsWith("$"))
        f.getType match {
          case t if t == classOf[String]      => tableColumns += f.getName -> "string"
          case t if t == classOf[Int]         => tableColumns += f.getName -> "integer"
          case t if t == classOf[Integer]     => tableColumns += f.getName -> "integer"
          case t if t == classOf[Boolean]     => tableColumns += f.getName -> "boolean"
          case t if t == classOf[Short]       => tableColumns += f.getName -> "short"
          case t if t == classOf[Double]      => tableColumns += f.getName -> "double"
          case t if t == classOf[Long]        => tableColumns += f.getName -> "long"
          case t if t == classOf[Float]       => tableColumns += f.getName -> "float"
          case t if t == classOf[Byte]        => tableColumns += f.getName -> "byte"

          // All other types are pickled to JSON and stored as a string
          case _                              => tableColumns += f.getName -> "string"
        }
    }

    var sqlStatement = s"create table $schema.$tableName ("
    var firstItem = true

    for((column, sqlType) <- tableColumns) {
      sqlStatement += s"${if (!firstItem) ", " else ""}$column $sqlType"
      firstItem = false
    }

    if(!primaryKey.equals(""))
      sqlStatement += s", primary key ($primaryKey)"

    sqlStatement += ")"

    try {
      crateClient.sql(sqlStatement).get
      exists(schema, objClass)(crateClient)
    }
    catch {
      case ex: Exception =>
        throw new IOException(s"Failed to make table for $schema.$tableName", ex)
    }
  }

  /**
   * Checks for the existence of a table matching
   * the CrateObject in the selected schema
   *
   * @param schema Schema to check under
   * @param objClass Object type
   * @param crateClient Crate client for connection to database
   * @return Boolean Returns true if the table exists, false if it doesn't
   */
  def exists(schema: String, objClass: Class[_ <: CrateObject])(implicit crateClient: CrateClient): Boolean = {
    val sqlResult = crateClient.sql(s"select * from information_schema.tables where table_name='${objClass.getSimpleName.toLowerCase}' and schema_name='$schema'").get()

    sqlResult.rowCount() == 1
  }

  /**
   * Run a select on the objects table a makes a list of the object from the results
   *
   * @param schema The schema to find the table under
   * @param conditional A SQL conditional to use when selecting
   * @param crateClient Crate client for the connection to database
   * @throws java.io.IOException Thrown when a failure occurred when selecting a list of objects from the database
   * @return
   */
  @throws(classOf[IOException])
  def select[T](schema: String, objClass: Class[_ <: CrateObject], conditional: String = "")(implicit crateClient: CrateClient): List[T] = {
    if(crateClient == null)
      throw new IOException("Requires implicit object crateClient to be initialized")

    if(!exists(schema, objClass)(crateClient))
      throw new IOException(s"Table for $schema.${objClass.getSimpleName.toLowerCase} does not exist yet!")

    try {
      var selectors = List.empty[String]

      objClass.getDeclaredFields.foreach { field =>
        if(!field.getName.startsWith("$"))
          selectors = selectors :+ field.getName
      }

      val sqlResult = crateClient.sql(s"select ${selectors.mkString(", ")} from $schema.${objClass.getSimpleName.toLowerCase} $conditional").get()
      var selectResult = List.empty[T]

      sqlResult.rows.foreach { args =>
        selectResult = selectResult :+ objClass.getConstructors()(0).newInstance(restoreTypes(objClass, args): _*).asInstanceOf[T]
      }

      selectResult
    }
    catch {
      case ex: Exception =>
        throw new IOException(s"Failed to select objects from $schema.${objClass.getSimpleName.toLowerCase}", ex)
    }
  }

  /**
   * Used by #restoreTypes to convert JSON strings to their orignal types
   */
  implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * Restores values from their JSON form if that is how they had to be stored
   *
   * @param objClass The primary class being used to sort data
   * @param argList The data from the database needing to be restored
   * @throws java.io.IOException Thrown when an unsupported type is detected
   * @return
   */
  @throws(classOf[IOException])
  private def restoreTypes(objClass: Class[_ <: CrateObject], argList: Array[Object]): Array[_ <: Object] = {
    var correctTypes = Array.empty[Object]
    val fields = objClass.getDeclaredFields

    for(i <- 0 until fields.length) {
      val giveType = argList(i).getClass
      val wantedType = fields(i).getType

      if(giveType != wantedType && giveType == classOf[String])
      {
        wantedType match {
          case t if t == classOf[Map[String, _]]      => correctTypes = correctTypes :+ read[Map[String, _]](argList(i).asInstanceOf[String])
          case t if t == classOf[List[_]]             => correctTypes = correctTypes :+ read[List[_]](argList(i).asInstanceOf[String])
          case t if t == classOf[Set[_]]              => correctTypes = correctTypes :+ read[Set[_]](argList(i).asInstanceOf[String])

          case _ =>
            throw new IOException(s"Unsupported type $wantedType")
        }
      }
      else
        correctTypes = correctTypes :+ argList(i)
    }

    correctTypes
  }

  /**
   * Drops the table related to this object if it exists
   *
   * @param schema The schema the table should exist under
   * @param crateClient Crate client for connection to database
   * @throws java.io.IOException Thrown when the table couldn't be dropped due to an error
   * @return Boolean Returns true if the table was dropped
   */
  @throws(classOf[IOException])
  def drop(schema: String, objClass: Class[_ <: CrateObject])(implicit crateClient: CrateClient): Boolean = {
    if(crateClient == null)
      throw new IOException("Requires implicit object crateClient to be initialized")

    if(!exists(schema, objClass)(crateClient))
      return false

    try {
      crateClient.sql(s"drop table $schema.${objClass.getSimpleName.toLowerCase}").get
      !exists(schema, objClass)(crateClient)
    }
    catch {
      case ex: Exception =>
        throw new IOException(s"Failed to drop table $schema.${objClass.getSimpleName.toLowerCase}", ex)
    }
  }
}
