import subprocess
import os
import time

while True:
    # Get the current working directory
    base_dir = os.getcwd()

    # Use relative paths to your scripts
    command1 = ["python3", os.path.join(base_dir, "web-scrapers/scrapers/scrapers/spiders/dfs.py")]
    command2 = ["python3", os.path.join(base_dir, "web-scrapers/curl/curl.py")]
    command3 = ["python3", os.path.join(base_dir, "web-scrapers/bet365.py")]
    command4 = ["python3", os.path.join(base_dir, "web-scrapers/draftkings.py")]

    # Start the processes
    process1 = subprocess.Popen(command1)
    process2 = subprocess.Popen(command2)
    process3 = subprocess.Popen(command3)
    process4 = subprocess.Popen(command4)

    # Wait for all processes to complete
    process1.wait()
    process2.wait()
    process3.wait()
    process4.wait()

    # Call main function to update the database
    os.system("python3 " + os.path.join(base_dir, "main.py"))

    # All processes are done
    print("All scripts have completed.")
    time.sleep(600)

    