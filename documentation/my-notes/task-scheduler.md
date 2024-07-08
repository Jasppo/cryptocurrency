# Create a Batch File to Run Your Script
@echo off
cd /d C:\path\to\your\project\
call poetry run python script.py

# Schedule the Task with Windows Task Scheduler
1. Open Task Scheduler: Search for "Task Scheduler" in the Start menu and open it.
2. Create a Basic Task:
   1. Click on "Create Basic Task" in the right pane.
   2. Name your task and provide a description, then click "Next".
   3. Choose "Daily" and click "Next".
3. Set the Trigger:
   1. Set it to start at 7 PM and recur every day.
   2. Click "Next".
4. Action:
   1. Choose "Start a program" and click "Next".
   2. In "Program/script", enter the path to your batch file (e.g., C:\path\to\your\run_script.bat).
   3. Leave "Start in" (optional) as the directory of your script if required or it can be left blank.
   4. Click "Next".
5. Finish: Review your settings and click "Finish" to create the task

# Tips
- Ensure your batch file works by double-clicking it before scheduling.
- Check any logs or outputs your script may generate to ensure it's running as expected under Task Scheduler.
- If your script interacts with the desktop or needs specific permissions, adjust the security options in the task's properties in Task Scheduler.