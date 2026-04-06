import great_expectations as gx
from sqlalchemy import text


DATASOURCE_NAME = "healthcare_postgres"
CONNECTION_STRING = "postgresql+psycopg2://healthcare:healthcare@localhost:5432/healthcare_dw"


def get_or_create_datasource(context):
    try:
        return context.data_sources.get(DATASOURCE_NAME)
    except Exception:
        return context.data_sources.add_postgres(
            name=DATASOURCE_NAME,
            connection_string=CONNECTION_STRING,
        )


def get_or_create_table_asset(datasource, asset_name: str, table_name: str):
    try:
        return datasource.get_asset(asset_name)
    except Exception:
        return datasource.add_table_asset(
            name=asset_name,
            table_name=table_name,
        )


def get_or_create_suite(context, suite_name: str):
    try:
        return context.suites.get(name=suite_name)
    except Exception:
        return context.suites.add(gx.ExpectationSuite(name=suite_name))


def get_validator(context, datasource, asset_name: str, table_name: str, suite_name: str):
    asset = get_or_create_table_asset(
        datasource=datasource,
        asset_name=asset_name,
        table_name=table_name,
    )
    batch_request = asset.build_batch_request()
    suite = get_or_create_suite(context, suite_name)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite.name,
    )
    return validator


def validate_patients(context, datasource) -> bool:
    validator = get_validator(
        context=context,
        datasource=datasource,
        asset_name="silver_patients_asset",
        table_name="silver_patients",
        suite_name="silver_patients_suite",
    )

    validator.expect_column_values_to_not_be_null("patient_id")
    validator.expect_column_values_to_be_unique("patient_id")
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=10_000_000)

    results = validator.validate()
    print("silver_patients:", results.success)
    return results.success


def validate_encounters(context, datasource) -> bool:
    validator = get_validator(
        context=context,
        datasource=datasource,
        asset_name="silver_encounters_asset",
        table_name="silver_encounters",
        suite_name="silver_encounters_suite",
    )

    validator.expect_column_values_to_not_be_null("encounter_id")
    validator.expect_column_values_to_be_unique("encounter_id")
    validator.expect_column_values_to_not_be_null("patient_id")
    validator.expect_column_values_to_not_be_null("provider_id")

    results = validator.validate()
    gx_success = results.success
    print("silver_encounters_gx:", gx_success)

    engine = validator.execution_engine.engine
    with engine.connect() as conn:
        invalid_rows = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM public.silver_encounters
                WHERE admit_time IS NOT NULL
                  AND discharge_time IS NOT NULL
                  AND admit_time > discharge_time
            """)
        ).scalar_one()

    sql_success = (invalid_rows == 0)
    # print(f"silver_encounters_time_order: {sql_success} (invalid_rows={invalid_rows})")

    return gx_success and sql_success


def validate_observations(context, datasource) -> bool:
    validator = get_validator(
        context=context,
        datasource=datasource,
        asset_name="silver_observations_asset",
        table_name="silver_observations",
        suite_name="silver_observations_suite",
    )

    validator.expect_column_values_to_not_be_null("observation_id")
    validator.expect_column_values_to_be_unique("observation_id")
    validator.expect_column_values_to_not_be_null("patient_id")
    validator.expect_column_values_to_be_between(
        "observation_value",
        min_value=0,
        max_value=300,
        mostly=0.99,
    )

    results = validator.validate()
    print("silver_observations:", results.success)
    return results.success


def main() -> None:
    context = gx.get_context()
    datasource = get_or_create_datasource(context)

    p = validate_patients(context, datasource)
    e = validate_encounters(context, datasource)
    o = validate_observations(context, datasource)

    if not all([p, e, o]):
        raise SystemExit("One or more Great Expectations validations failed.")

    print("All Great Expectations validations passed.")


if __name__ == "__main__":
    main()