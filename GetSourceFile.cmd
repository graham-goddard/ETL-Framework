@echo off
setlocal enableDelayedExpansion

set SOURCEFILE=%1
set TARGET=%2

set SQL_OBJECTS_DIR=.

:: find files - first char will be backslash
echo %SOURCEFILE% | findstr /B /V /L /C:"\\" >nul
if %ERRORLEVEL%==0 goto end

:: print scriptname
echo. >> %TARGET%
echo PRINT '%SOURCEFILE%' >> %TARGET%
echo GO >> %TARGET%
echo. >> %TARGET%

IF NOT EXIST "%SQL_OBJECTS_DIR%%SOURCEFILE%" (
	@echo %SOURCEFILE% - Error
	@echo.
	@echo Unable to find "%SQL_OBJECTS_DIR%%SOURCEFILE%"
	goto abort
)

:: concatenate files
COPY /Y /B %TARGET% + "%SQL_OBJECTS_DIR%%SOURCEFILE%" %TARGET% >nul
IF !ERRORLEVEL! NEQ 0 (
	@echo Error copying to %TARGET%. Operation aborted.
	goto abort
)

@echo %SOURCEFILE% - Ok

goto end

:abort
:: clean up
del /Q %TARGET%

@echo Script aborted!
exit /B 2

:end
