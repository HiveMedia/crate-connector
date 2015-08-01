Scala Connector
---------------

Scala Connector is a helper library to make using CrateDB and Scala that little bit simpler, it cuts down development
time by allowing the storage and retrieval of case classes in CrateDB


Version
-------

`1.1.0`


Usage
-----

First off, start by adding the library to your dependencies like so

    libraryDependencies += "au.net.hivemedia" %% "crate-connector" % "1.1.0"

Then on all classes that you would like to store in your database, just extend with `CrateObject`.

    case class User(id: Int, username: String, email: String, password: String) extends CrateObjec

### Creating Table from Object

    CrateConnector.create("mydb", classOf[User], "id")

### Checking for Table Existence

    CrateConnector.exists("mydb", classOf[User])

### Dropping Table

    CrateConnector.drop("mydb", classOf[User])

### Add Item to Table

    // Defines the item to be inserted into the table
    val newUser = User(1, "testuser", "testuser@example.com", "SimplePassword")

    // Call the insert method on the item
    newUser.insert("mydb")

### Selecting Items

    CrateConnector.select[User]("mydb", classOf[User], "where id=1")


License and Authors
-------------------

|                        |                                                 |
|:-----------------------|:------------------------------------------------|
| **Author**             | Liam Haworth <liam.haworth@hivemedia.net.au>    |
|                        |                                                 |
| **Copyright**          | Copyright (c) 2015, Hive Media Productions      |

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.