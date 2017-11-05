@echo off
if not "%1" == "--skip-yarn" call "%~dp0yarn.cmd" --pure-lockfile --mutex network
call "%~dp0node_modules\.bin\ts-node.cmd"  --ignore "\.js$" --project "%~dp0buildsystem" "%~dp0buildsystem\index.ts" --root-path=%~dp0 %*
