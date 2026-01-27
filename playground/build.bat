@echo off
REM toon4s Playground Build Script (Windows)
REM 
REM Usage:
REM   build.bat              - Fast build (development)
REM   build.bat --prod       - Production build (optimized)
REM   build.bat --watch      - Watch mode (auto-rebuild)

setlocal

cd /d "%~dp0.."

echo.
echo 🎨 toon4s Playground Builder
echo ==============================
echo.

if "%1"=="--prod" goto prod
if "%1"=="--watch" goto watch
goto fast

:fast
echo 📦 Building playground (fast mode)...
call sbt playground/fastLinkJS
if errorlevel 1 (
    echo ❌ Build failed!
    exit /b 1
)
echo.
echo ✅ Build complete!
echo.
echo 🌐 Open playground:
echo    file:///%~dp0src\main\resources\index.html
echo.
goto end

:prod
echo 📦 Building playground (production mode)...
call sbt playground/fullLinkJS
if errorlevel 1 (
    echo ❌ Build failed!
    exit /b 1
)
echo.
echo ✅ Production build complete!
echo.
echo ⚠️  Don't forget to update index.html to use:
echo    toon4s-playground-opt.js
echo.
echo 🌐 Open playground:
echo    file:///%~dp0src\main\resources\index.html
echo.
goto end

:watch
echo 👀 Starting watch mode...
echo    Changes will auto-compile on save
echo    Press Ctrl+C to stop
echo.
call sbt "~playground/fastLinkJS"
goto end

:end
endlocal
