﻿using System.Reflection;
using System.Resources;
using System.Runtime.InteropServices;

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Retail")]
#endif
[assembly: AssemblyCompany("Wintegra®")]
[assembly: AssemblyProduct("This package contain driver set for use with IBM DB2 database. You can use the ODBC or JDBC driver.")]
[assembly: AssemblyCopyright("Copyright © Wintegra® 2016. All rights reserved.")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

[assembly: ComVisible(false)]
[assembly: NeutralResourcesLanguage("en")]

[assembly: AssemblyVersionAttribute("1.1.0.0")]
[assembly: AssemblyInformationalVersionAttribute("1.1.0.0-devel")]
[assembly: AssemblyFileVersionAttribute("1.1.0.0")]
