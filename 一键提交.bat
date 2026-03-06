@echo off
setlocal enabledelayedexpansion
title Git Auto-Pusher

echo =========================================
echo Git Lazy-Push Protocol V2.0 initialized
echo =========================================

:: [Zero-Setup] Check if .git exists
if exist .git goto check_remote
echo [INFO] .git not found. Initializing...
git init
if %errorlevel% neq 0 (
    echo [ERROR] git init failed.
    pause
    exit /b %errorlevel%
)

:check_remote
:: [Smart Remote] Check if origin exists
git remote | find "origin" >nul
if %errorlevel% equ 0 goto prompt_commit

echo [INFO] Remote 'origin' not found.
:prompt_url
set "remote_url="
set /p remote_url=">>> Enter remote repository URL (e.g., https://github.com/user/repo.git): "
if "%remote_url%"=="" (
    echo [ERROR] URL cannot be empty.
    goto prompt_url
)
git remote add origin %remote_url%

:prompt_commit
echo [INFO] Working tree status:
git status -s

:: [Simplify] Prompt for commit message with default fallback
set "msg=Auto commit"
set /p msg=">>> Enter commit message (Press ENTER for default: 'Auto commit'): "

echo [INFO] Adding files...
git add .
echo [INFO] Committing...
git commit -m "%msg%"

:: [Dynamic Branching] Get current branch safely
echo [INFO] Fetching current branch...
set "branch="
for /f "tokens=*" %%a in ('git branch --show-current') do set "branch=%%a"
if "%branch%"=="" set "branch=main"
echo [INFO] Current branch is %branch%.

:: [Smart Push] Push and fallback to -u
echo [INFO] Pushing to origin %branch%...
git push origin %branch%
if %errorlevel% neq 0 (
    echo [INFO] Standard push failed. Attempting with upstream tracking...
    git push -u origin %branch%
)

echo.
echo =========================================
echo Process completed.
echo =========================================
pause
