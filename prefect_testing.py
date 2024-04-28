from prefect_dbt.cli.commands import DbtCoreOperation
from prefect import flow

@flow
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["dbt run"],
        project_dir="jaffle_shop",
        profiles_dir="~/.dbt"
    ).run()
    return result

if __name__ == "__main__":
    trigger_dbt_flow()