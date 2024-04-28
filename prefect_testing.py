from prefect_dbt.cli.commands import DbtCoreOperation
from prefect import flow

@flow
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["dbt run"],
        project_dir="C:/Users/ryanh/jaffle_shop",
        profiles_dir="C:/Users/ryanh/.dbt/profiles.yml"
    ).run()
    return result

if __name__ == "__main__":
    trigger_dbt_flow()