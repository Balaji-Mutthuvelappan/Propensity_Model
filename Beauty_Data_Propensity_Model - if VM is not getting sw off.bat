@echo off
REM --- run_scheduled_script.bat - Executes Python script from Conda 'py39' env and shuts down VM ---
REM
REM This batch file is designed to be run as a Windows Scheduled Task, triggered when the VM starts up.
REM Its primary purpose is to:
REM 1. Log its own execution steps.
REM 2. Initialize and activate a specific Anaconda/Conda environment ('py39' in this case).
REM 3. Run a specified Python script ('sentiment_model.py') using that Conda environment.
REM 4. Log the Python script's output and any errors.
REM 5. Automatically shut down the VM to save cloud computing costs.

REM --- 1. Define General Log File Paths ---
REM MASTER_LOG_DIR: Directory where this batch script's own log file will be stored.
set "MASTER_LOG_DIR=C:\Logs"
REM MASTER_LOG_FILE: The full path to this batch script's primary log file.
set "MASTER_LOG_FILE=%MASTER_LOG_DIR%\startup_script_output.log"

REM --- 2. Create Master Log Directory if it doesn't exist ---
REM Checks if the C:\Logs directory exists. If not, it creates it.
if not exist "%MASTER_LOG_DIR%" mkdir "%MASTER_LOG_DIR%"

REM --- 3. Get Current Timestamp for Logging ---
REM These two 'for /f' loops capture the current date and time.
REM %%a, %%b, %%c, %%d are loop variables to parse date components.
for /f "tokens=1-4 delims=/ " %%a in ('date /t') do set "d=%%c-%%a-%%b"
for /f "tokens=1-2 delims=:." %%a in ('time /t') do set "t=%%a:%%b"
REM Combine date and time into a single timestamp variable.
set "CURRENT_TIMESTAMP=%d% %t%"
REM Log the start of the batch script to the master log file. '>>' appends to the file.
echo %CURRENT_TIMESTAMP%: Batch script started. >> "%MASTER_LOG_FILE%"

REM --- 4. Conda Environment and Python Script Configuration (YOUR CUSTOM VALUES) ---
REM IMPORTANT: Adjust these paths and names to match your specific setup on the VM.

REM CONDA_BASE_PATH: Full path to your base Anaconda or Miniconda installation directory.
set "CONDA_BASE_PATH=C:\Users\balaji_muthuvelapp77\anaconda3"
REM CONDA_ENV_NAME: The exact name of the Conda environment you want to activate (e.g., 'base', 'py39', 'my_env').
set "CONDA_ENV_NAME=py39"
REM PYTHON_SCRIPT_PATH: The full path to your main Python script that needs to be executed.
set "PYTHON_SCRIPT_PATH=C:\Users\balaji_muthuvelapp77\Deployment on VM\Propensity_Model\Main_propensity_score_with_category.py"
REM PYTHON_SCRIPT_OUTPUT_LOG: The full path to the log file where your Python script's 'print()' output and any errors will be redirected.
set "PYTHON_SCRIPT_OUTPUT_LOG=C:\Users\balaji_muthuvelapp77\Deployment on VM\Propensity_Model\logs\sentiment_analysis_log.txt"
REM ==============================================================================

REM --- 5. Create Python Script Output Log Directory ---
REM This block extracts the directory path from PYTHON_SCRIPT_OUTPUT_LOG.
REM It then creates this directory if it doesn't already exist.
for %%F in ("%PYTHON_SCRIPT_OUTPUT_LOG%") do set "PYTHON_LOG_DIR=%%~dpF"
if not exist "%PYTHON_LOG_DIR%" mkdir "%PYTHON_LOG_DIR%"

REM --- 6. Initialize Conda and Activate Specific Environment ---
echo %CURRENT_TIMESTAMP%: Initializing Conda and activating environment '%CONDA_ENV_NAME%'. >> "%MASTER_LOG_FILE%"

REM CALL "%CONDA_BASE_PATH%\Scripts\activate.bat":
REM This command is crucial. It runs the Conda initialization script.
REM 'CALL' is used to execute another batch script from within this one without terminating the current script.
CALL "%CONDA_BASE_PATH%\Scripts\activate.bat"
REM IF %ERRORLEVEL% NEQ 0: Checks if the previous command (conda init) failed (non-zero exit code).
IF %ERRORLEVEL% NEQ 0 (
    echo %CURRENT_TIMESTAMP%: ERROR - Conda init failed or activate.bat not found. Check CONDA_BASE_PATH. >> "%MASTER_LOG_FILE%"
    GOTO :FINALIZE_AND_SHUTDOWN REM Jumps to the shutdown section if an error occurs.
)

REM CALL conda activate %CONDA_ENV_NAME%:
REM Activates the specified Conda environment. This command becomes available after 'conda init'.
CALL conda activate %CONDA_ENV_NAME%
IF %ERRORLEVEL% NEQ 0 (
    echo %CURRENT_TIMESTAMP%: ERROR - Failed to activate Conda environment '%CONDA_ENV_NAME%'. Check CONDA_ENV_NAME. >> "%MASTER_LOG_FILE%"
    GOTO :FINALIZE_AND_SHUTDOWN REM Jumps to the shutdown section if an error occurs.
)
echo %CURRENT_TIMESTAMP%: Conda environment '%CONDA_ENV_NAME%' activated successfully. >> "%MASTER_LOG_FILE%"

REM --- 7. Run Python Script ---
echo %CURRENT_TIMESTAMP%: Running Python script: "%PYTHON_SCRIPT_PATH%" >> "%MASTER_LOG_FILE%"
REM "%CONDA_BASE_PATH%\envs\%CONDA_ENV_NAME%\python.exe" "%PYTHON_SCRIPT_PATH%" >> "%PYTHON_SCRIPT_OUTPUT_LOG%" 2>&1:
REM This directly executes the 'python.exe' from within your specific Conda environment.
REM It ensures the correct Python interpreter with its associated packages is used.
REM '>>' redirects standard output to the specified log file (appends).
REM '2>&1' redirects standard error (channel 2) to the same location as standard output (channel 1),
REM meaning both print statements and error messages from your Python script go to the same log.
"%CONDA_BASE_PATH%\envs\%CONDA_ENV_NAME%\python.exe" "%PYTHON_SCRIPT_PATH%" >> "%PYTHON_SCRIPT_OUTPUT_LOG%" 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo %CURRENT_TIMESTAMP%: ERROR - Python script "%PYTHON_SCRIPT_PATH%" failed with exit code: %ERRORLEVEL%. >> "%MASTER_LOG_FILE%"
) else (
    echo %CURRENT_TIMESTAMP%: Python script completed successfully. >> "%MASTER_LOG_FILE%"
)

REM --- 8. Finalize Logging and Initiate VM Shutdown ---
:FINALIZE_AND_SHUTDOWN
REM This is a label that 'GOTO' commands jump to in case of errors or successful completion.
echo %CURRENT_TIMESTAMP%: Script finished. No shutdown requested. >> "%MASTER_LOG_FILE%"
REM timeout /t 5 /nobreak > NUL:
REM Pauses the script for 5 seconds. This is important to give the system a moment to finish writing all log entries
REM before the VM is abruptly shut down, preventing truncated log files. '> NUL' suppresses timeout's own output.
REM timeout /t 5 /nobreak > NUL
REM shutdown /s /f /t 0:
REM This command shuts down the computer.
REM /s: Shuts down the computer.
REM /f: Forces running applications to close without warning.
REM /t 0: Sets the time-out period before shutdown to 0 seconds (immediate shutdown after the 5-second delay).
REM shutdown /s /f /t 0