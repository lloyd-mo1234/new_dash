
import shutil
import os

# Define source and destination paths
source_path = r"\\flln01\g_gm_ln$\IRO London\IRD\lloyd\curve data\testing.txt"
destination_dir = r"C:\BAppGeneral"
destination_path = os.path.join(destination_dir, "testing.txt")

# Ensure destination directory exists
os.makedirs(destination_dir, exist_ok=True)

# Check if source file exists
if os.path.exists(source_path):
    try:
        # Move the file to the destination
        shutil.move(source_path, destination_path)
        print(f"File moved successfully to {destination_path}")
    except Exception as e:
        print(f"Error moving file: {e}")
else:
    print("Source file does not exist.")
