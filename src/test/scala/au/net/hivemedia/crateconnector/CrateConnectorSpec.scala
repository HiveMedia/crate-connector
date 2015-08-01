package au.net.hivemedia.crateconnector

import java.io.IOException

import io.crate.client.CrateClient
import org.scalatest.{Matchers, FlatSpec}

/**
 * Test Case Class to test the storage and retrevial of items from database
 */
case class TestObject(testInt: Int, testString: String, testBoolean: Boolean,
                      testShort: Short, testDouble: Double, testLong: Long,
                      testFloat: Float, testByte: Byte, testList: List[_],
                      testMap: Map[String, _], testSet: Set[_]) extends CrateObject

/**
 * Scala Test Spec used to test
 * the functionality of CrateConnector
 *
 * @author Liam Haworth
 * @version 1.0
 */
class CrateConnectorSpec extends FlatSpec with Matchers {

  val crateDatabaseServer = "localhost:4300"

  val testObject = TestObject(Int.MaxValue, "Testing123", true, Short.MaxValue, 9.87654321D, Long.MaxValue, 1.2345f, 0x32, List(1, "two", 0x03, 0.4f), Map("Test" -> "Map"), Set("1", 2, 0x3))

  "CrateObject" should "be the superclass of TestObject" in {
    testObject.getClass.getSuperclass should equal(classOf[CrateObject])
  }

  it should "expose helper methods on objects extending it" in {
    testObject.getClass.getSuperclass.getDeclaredMethods.map(_.getName) should contain ("insert")
    testObject.getClass.getSuperclass.getDeclaredMethods.map(_.getName) should contain ("update")
  }

  "CreateConnector" should "throw an IOException when no CrateClient is defined" in {
    a [IOException] should be thrownBy {
      CrateConnector.create("testdb", classOf[TestObject])(null)
    }
  }

  it should "create a table in test database based on case class" in {
    implicit val crateClient = new CrateClient(crateDatabaseServer)

    CrateConnector.create("testdb", classOf[TestObject]) should be (true)

    val sqlResult = crateClient.sql("select * from information_schema.tables where table_name='testobject' and schema_name='testdb'").get

    sqlResult.rowCount() should be (1)
  }

  it should "be able to test the existence of the table" in {
    implicit val crateClient = new CrateClient(crateDatabaseServer)

    CrateConnector.exists("testdb", classOf[TestObject]) should be (true)
  }

  it should "insert an object into a table" in {
    implicit val crateClient = new CrateClient(crateDatabaseServer)

    var sqlResult = crateClient.sql("select * from testdb.testobject").get
    sqlResult.rowCount() should be (0)

    testObject.insert("testdb")
    Thread.sleep(2500)

    sqlResult = crateClient.sql("select * from testdb.testobject").get
    sqlResult.rowCount() should be (1)
  }

  it should "make a list of objects from database" in {
    implicit val crateClient = new CrateClient(crateDatabaseServer)

    val objects = CrateConnector.select[TestObject]("testdb", classOf[TestObject])

    objects.size should be (1)
  }

  it should "update a record from an object and conditional" in {
    implicit val crateClient = new CrateClient(crateDatabaseServer)

    var objects = CrateConnector.select[TestObject]("testdb", classOf[TestObject])

    objects(0).testInt should equal(Int.MaxValue)

    val updatedObject = TestObject(Int.MinValue, "Testing123", true, Short.MaxValue, 9.87654321D, Long.MaxValue, 1.2345f, 0x32, List(1, "two", 0x03, 0.4f), Map("Test" -> "Map"), Set("1", 2, 0x3))

    updatedObject.update("testdb", "where testString = 'Testing123'")
    Thread.sleep(2500)

    objects = CrateConnector.select[TestObject]("testdb", classOf[TestObject])

    objects(0).testInt should equal(Int.MinValue)
  }

  it should "drop the table" in {
    implicit val crateClient = new CrateClient(crateDatabaseServer)

    CrateConnector.exists("testdb", classOf[TestObject]) should be (true)

    CrateConnector.drop("testdb", classOf[TestObject]) should be (true)
  }
}
