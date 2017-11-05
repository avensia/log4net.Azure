@echo off

echo %PATH% | %WINDIR%\system32\find.exe /i "%~dp0" > nul || set PATH=%~dp0;%PATH%

set "toolsPath=%~dp0tools\"
for /f "delims=" %%F in ('dir "%toolsPath%yarn*.js" /b') do set "yarnJsPath=%%F"

"%~dp0node.cmd" "%toolsPath%%yarnJsPath%" %*
