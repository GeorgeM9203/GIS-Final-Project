import subprocess
import sys

def main():
    scripts = [
        "datacollection.py",
        "cleaning.py",
        "spatial_analysis.py",
        "visualization.py"
    ]

    for script in scripts:
        result = subprocess.run([sys.executable, script])
        
        if result.returncode != 0:
            print(f"Error: {script} failed. Pipeline stopped.")
            exit(1)

    print("Pipeline finished successfully! Check the data/results/ folder.")

if __name__ == "__main__":
    main()
