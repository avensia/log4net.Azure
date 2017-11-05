@echo off

echo %PATH% | %WINDIR%\system32\find.exe /i "%~dp0" > nul || set PATH=%~dp0;%PATH%

"%~dp0tools\node.exe" %*
