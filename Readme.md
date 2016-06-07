# Wintegra.Data

Wrapper over IBM DB2 ODBC DRIVER over Microsoft ODBC provider V2.0.0.0 in framework .NET V2.0

## Features

* Compatible with [Dapper](https://github.com/StackExchange/dapper-dot-net).
* Can find {IBM DB2 ODBC DRIVER} into Windows Registry.
* Can read and write BLOB, DBCLOB, CLOB and XML fields.
* Replace named parameters to the question mark (?) placeholder for passing parameters to a SQL Statement or a stored procedure (use with caution).

# Usage

You need download and install IBM DB2 ODBC CLI driver, for example with [Download initial Version 10.5 clients and drivers](http://www-01.ibm.com/support/docview.wss?uid=swg21385217)

After you can create connection to DB2 as:

```cs
const string connectionString = "Driver={IBM DB2 ODBC DRIVER};DataBase=DB1; HostName=127.0.0.1; Protocol=TCPIP;Port=50000;Uid=db2admin;Pwd=db2admin;CurrentSchema=DB01;DB2NETNamedParam=1;HostVarParameters=1";
using (var db = new Db2.Data.Db2Client.Db2Connection(connectionString))
{
  db.Open();
  using (var command = db.CreateCommand())
  {
    command.CommandText = "SELECT * FROM TABLE(VALUES('It is work')) AS T(LOG)";
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
It is work
</pre>

