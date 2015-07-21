package au.net.hivemedia.crateconnector

import java.io.IOException

import io.crate.client.CrateClient

import scala.pickling.Defaults._
import scala.pickling.json._

/**
 * CrateObject supplies methods
 * for objects to store them in
 * a Crate Database
 *
 * @author Liam Haworth
 * @version 1.0
 */
case class CrateObject(primaryKey: String = null) {

  import CrateConnector._

  /**
   * Converts a object of any type to a value that can be stored in the database
   *
   * @param obj Object to be converted to a value string
   * @return String
   */
  private def convertToString(obj: AnyRef): String = {
    obj.getClass match {
      case t if t == classOf[String]      => obj.asInstanceOf[String]
      case t if t == classOf[Int]         => obj.toString
      case t if t == classOf[Integer]     => obj.toString
      case t if t == classOf[Boolean]     => if (obj.asInstanceOf[Boolean]) "0" else "1"
      case t if t == classOf[Short]       => obj.toString
      case t if t == classOf[Double]      => obj.toString
      case t if t == classOf[Long]        => obj.toString
      case t if t == classOf[Float]       => obj.toString
      case t if t == classOf[Byte]        => obj.toString
      case _                              => obj.pickle.value
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
      f.setAccessible(true)
      insertData += f.getName -> s"'${convertToString(f.get(this))}'"
    }

    val sqlStatement = s"insert into $schema.${this.getClass.getSimpleName.toLowerCase}(${insertData.keys.mkString(", ")}) values(${insertData.values.mkString(", ")})"

    try {
      crateClient.sql(sqlStatement)
    }
    catch {
      case ex: Exception =>
        throw new IOException(s"Failed to insert object into $schema.${this.getClass.getSimpleName.toLowerCase}", ex)
    }
  }


}

