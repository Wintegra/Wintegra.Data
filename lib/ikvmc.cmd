@echo off
cls
"%programfiles(x86)%\ikvmc.exe" -classloader:ikvm.runtime.AppDomainAssemblyClassLoader -target:library db2jcc4.jar db2jcc_license_cu.jar -out:db2jcc4.dll