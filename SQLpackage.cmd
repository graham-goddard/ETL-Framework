@echo off

setlocal enableDelayedExpansion

set MAKEFILE=%1

if "%MAKEFILE%"=="" set MAKEFILE=makefile.txt

@echo.
set /p MAKEFILE="Enter the MAKEFILE filename [%MAKEFILE%]: "


IF NOT EXIST "%MAKEFILE%" (
	@echo.
	@echo Unable to find MAKEFILE
	goto end
)

FOR /F "tokens=1,2,3* delims==" %%a IN ('findstr /I /C:"<PACKAGE>=" %MAKEFILE%') DO set PACKAGE=%%b
FOR /F "tokens=1,2,3* delims==" %%a IN ('findstr /I /C:"<VERSION>=" %MAKEFILE%') DO set VERSION=%%b

echo.
set /p PACKAGE="Enter the name for this PACKAGE [%PACKAGE%]: "

echo.
set /p VERSION="Enter the VERSION for this package [%VERSION%]: "

echo.
choice /C YN /N /M "Ready to create package? [Y/N]: "
if %ERRORLEVEL%==2 goto end

FOR /F %%a in ("%MAKEFILE%") do set RELEASE_DIR=%%~dpa

set RELEASE_FILE=%PACKAGE%
set TARGET="%RELEASE_DIR%%RELEASE_FILE%.sql"

:: initialise target file
echo -- Package: %PACKAGE%  > %TARGET%
echo -- Version: %VERSION% >> %TARGET%

for /F %%F in (%MAKEFILE%) do	(
	call GetSourceFile.cmd %%F %TARGET%
	if !ERRORLEVEL! NEQ 0 GOTO end
	)

:: clean up

echo.
echo Created %TARGET%
echo.
echo SQLpackage complete
echo.
pause

goto end

:Usage
@echo.
@echo Usage : %0
@echo.
@echo The %0 batch script packages files into a single SQL Server script file.
goto end

:end
