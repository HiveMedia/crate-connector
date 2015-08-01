package au.net.hivemedia.crateconnector

import java.io.IOException
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import io.crate.client.CrateClient

/**
 * CrateObject supplies methods
 * for objects to store them in
 * a Crate Database
 *
 * @author Liam Haworth
 * @version 1.0
 */
abstract class CrateObject {

  import CrateConnector._

  /**
   * Converts a object of any type to a value that can be stored in the database
   *
   * @param obj Object to be converted to a value string
   * @return String
   */
  private def convertToString(obj: AnyRef, objType: Class[_]): String = {
    objType match {
      case t if t == classOf[String]      => obj.asInstanceOf[String]
      case t if t == classOf[Int]         => obj.toString
      case t if t == classOf[Integer]     => obj.toString
      case t if t == classOf[Boolean]     => if (obj.asInstanceOf[Boolean]) "true" else "false"
      case t if t == classOf[Short]       => obj.toString
      case t if t == classOf[Double]      => obj.toString
      case t if t == classOf[Long]        => obj.toString
      case t if t == classOf[Float]       => obj.toString
      case t if t == classOf[Byte]        => obj.toString

      case _                              =>
        implicit val formats = Serialization.formats(NoTypeHints)
        write(obj)(formats)
    }
  }

  /**
   * Insets a object into its table in the selected schema
   *
   * @param schema The schema the object should be stored under
   * @param crateClient Crate client for connection to database
   * @throws java.io.IOException Thrown when the object couldn't be stored in the database
   */
  @throws(classOf[IOException])
  def insert(schema: String)(implicit crateClient: CrateClient): Unit = {
    if(crateClient == null)
      throw new IOException("Requires implicit object crateClient to be initialized")

    if(!exists(schema, this.getClass)(crateClient))
      throw new IOException(s"Table for $schema.${this.getClass.getSimpleName.toLowerCase} does not exist yet!")

    var insertData = Map.empty[String, String]

    this.getClass.getDeclaredFields.foreach { f =>
      if(!f.getName.startsWith("$")) {
        f.setAccessible(true)
        insertData += f.getName -> s"'${convertToString(f.get(this), f.getType)}'"
      }
    }

    val sqlStatement = s"insert into $schema.${this.getClass.getSimpleName.toLowerCase}(${insertData.keys.mkString(", ")}) values(${insertData.values.mkString(", ")})"

    try {
      crateClient.sql(sqlStatement).get
    }
    catch {
      case ex: Exception =>
        throw new IOException(s"Failed to insert object into $schema.${this.getClass.getSimpleName.toLowerCase}", ex)
    }
  }

  /**
   * Updates a objects record on the database with the data provided by the parent object
   *
   * @param schema The schema the object is stored under
   * @param conditional The conditional defining which records should be updated
   * @param crateClient Crate client for connection to database
   * @throws java.io.IOException Thrown when the records couldn't be updated
   */
  def update(schema: String, conditional: String = "")(implicit crateClient: CrateClient): Unit = {
    if(crateClient == null)
      throw new IOException("Requires implicit object crateClient to be initialized")

    if(!exists(schema, this.getClass)(crateClient))
      throw new IOException(s"Table for $schema.${this.getClass.getSimpleName.toLowerCase} does not exist yet!")

    var updateData = Map.empty[String, String]

    this.getClass.getDeclaredFields.foreach { f =>
      if(!f.getName.startsWith("$")) {
        f.setAccessible(true)
        updateData += f.getName -> s"'${convertToString(f.get(this), f.getType)}'"
      }
    }

    val sqlStatement = s"update $schema.${this.getClass.getSimpleName.toLowerCase} set ${updateData.mkString(", ").replace("->", "=")} $conditional"

    try {
      crateClient.sql(sqlStatement).get
    }
    catch {
      case ex: Exception =>
        throw new IOException(s"Failed to update records in $schema.${this.getClass.getSimpleName.toLowerCase}", ex)
    }
  }
}

