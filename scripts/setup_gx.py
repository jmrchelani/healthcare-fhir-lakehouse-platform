import great_expectations as gx


def main() -> None:
    context = gx.get_context()

    datasource_name = "healthcare_postgres"

    try:
        context.data_sources.get(datasource_name)
        print(f"Datasource '{datasource_name}' already exists.")
    except Exception:
        context.data_sources.add_postgres(
            name=datasource_name,
            connection_string="postgresql+psycopg2://healthcare:healthcare@localhost:5432/healthcare_dw",
        )
        print(f"Datasource '{datasource_name}' created.")


if __name__ == "__main__":
    main()