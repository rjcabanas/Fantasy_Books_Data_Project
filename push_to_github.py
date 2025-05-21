import os
import subprocess
from datetime import date

def push_to_github(repo_path, local_file_path, commit_msg=f"Update Data File - {date.today}"):
       os.chdir(repo_path)
       subprocess.run(["git", "add", local_file_path])
       subprocess.run(["git", "commit", "-m", commit_msg])
       subprocess.run(["git", "push"])

