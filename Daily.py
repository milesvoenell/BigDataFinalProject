# Daily.py
import subprocess
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths to your scripts
VALIDATION_SCRIPT = Path("Data/Validation.py")
LOAD_SCRIPT = Path("Data/load_to_opensearch.py")

def run_script(script_path: Path) -> None:
    """Run a Python script in the current virtual environment."""
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return

    logger.info(f"Running script: {script_path}")
    try:
        subprocess.run([sys.executable, str(script_path)], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_path}")
        logger.error(f"Exit code: {e.returncode}")
        if e.stderr:
            logger.error(f"Error output: {e.stderr.decode()}")
    except ModuleNotFoundError as e:
        logger.error(f"Module not found: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def run_daily_etl() -> None:
    logger.info("Starting full daily ETL pipeline")

    # Step 1: Validation
    run_script(VALIDATION_SCRIPT)

    # Step 2: Load to OpenSearch
    run_script(LOAD_SCRIPT)

    logger.info("Daily ETL pipeline finished")

if __name__ == "__main__":
    run_daily_etl()
