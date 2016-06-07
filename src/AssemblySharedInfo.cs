using System.Reflection;
using System.Resources;
using System.Runtime.InteropServices;

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Retail")]
#endif
[assembly: AssemblyCompany("Wintegra®")]
[assembly: AssemblyProduct("Wrapper over IBM DB2 ODBC DRIVER over Microsoft ODBC provider V2.0.0.0 in framework .NET V2.0")]
[assembly: AssemblyCopyright("Copyright © Wintegra® 2016. All rights reserved.")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

[assembly: ComVisible(false)]
[assembly: NeutralResourcesLanguage("en")]

[assembly: AssemblyVersionAttribute("1.0.0.0")]
[assembly: AssemblyInformationalVersionAttribute("1.0.0.0-devel")]
[assembly: AssemblyFileVersionAttribute("1.0.0.0")]
