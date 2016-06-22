# Wintegra.Data

This package contain driver set for use with IBM DB2 database.
You can use the ODBC or JDBC driver.

## Features

* Compatible with [Dapper](https://github.com/StackExchange/dapper-dot-net).
* Compatible with multiple SQL statements in one query.
* Can find {IBM DB2 ODBC DRIVER} into Windows Registry for ODBC implementation.
* Can read and write BLOB, DBCLOB, CLOB and XML fields.
* Replace named parameters to the question mark (?) placeholder for passing parameters to a SQL Statement or a stored procedure (use with caution).


## ODBC

Wrapper over IBM DB2 ODBC DRIVER over Microsoft ODBC provider V2.0.0.0 in framework .NET V2.0

You need download and install IBM DB2 ODBC CLI driver, for example with [Download initial Version 10.5 clients and drivers](http://www-01.ibm.com/support/docview.wss?uid=swg21385217)

## JDBC

Package contain db2jcc4.dll created from db2jcc4.jar and db2jcc_license_cu.jar with the help of the command:
```bash
ikvmc.exe -classloader:ikvm.runtime.AppDomainAssemblyClassLoader -target:library db2jcc4.jar db2jcc_license_cu.jar -out:db2jcc4.dll
```



# Usage

After you can create connection to DB2 as:

## ODBC

```cs
const string connectionString = "Driver={IBM DB2 ODBC DRIVER};DataBase=DB1; HostName=127.0.0.1; Protocol=TCPIP;Port=50000;Uid=db2admin;Pwd=db2admin;CurrentSchema=DB01;DB2NETNamedParam=1;HostVarParameters=1";
using (var db = new Db2.Data.Db2Client.Db2Connection(connectionString))
{
  db.Open();
  using (var command = db.CreateCommand())
  {
    command.CommandText = "SELECT * FROM TABLE(VALUES('Used wrapper over ODBC')) AS T(LOG)";
    using (var reader = command.ExecuteReader())
    {
      reader.Read();
      Console.WriteLine(reader.GetString(0));
    }
  }
}
```
Output:
<pre>
Used wrapper over ODBC
</pre>

## JDBC

```cs
const string connectionString = "jdbc:db2://192.168.72.135:50000/DB1:currentSchema=DB01;user=root;password=password;fullyMaterializeLobData=true;";
using (var db = new Db2.Data.jdbc.Db2Connection(connectionString))
{
  db.Open();
  using (var command = db.CreateCommand())
  {
    command.CommandText = "SELECT * FROM TABLE(VALUES('Used JDBC level 4')) AS T(LOG)";
    using (var reader = command.ExecuteReader())
    {
      reader.Read();
      Console.WriteLine(reader.GetString(0));
    }
  }
}
```
Output:
<pre>
Used JDBC level 4
</pre>