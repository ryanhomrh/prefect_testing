import prefect
from prefect import task, Flow
import subprocess

# Define the paths to your dbt project directory and profiles directory
DBT_PROJECT_DIR = "C:/Users/ryanh/jaffle_shop"
DBT_PROFILES_DIR = "C:/Users/ryanh/.dbt/profiles.yml"

# Define Prefect task for running dbt
@task
def run_dbt():
    try:
        # Execute dbt run command
        subprocess.run(["dbt", "run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR], check=True)
        prefect.logger.info("dbt run completed successfully")
        return True  # Indicate success
    except subprocess.CalledProcessError as e:
        prefect.logger.error(f"Error running dbt: {e}")
        return False  # Indicate failure

# Define the Prefect flow
with Flow("dbt_run_flow") as flow:
    dbt_success = run_dbt()

# Run the flow
flow_state = flow.run()

# Check if the flow ran successfully and dbt command executed
if flow_state.is_successful() and dbt_success.result:
    print("Flow ran successfully and dbt command executed.")
else:
    print("Flow failed or dbt command execution failed.")
