# ecb-exchange-rates

ECB Exchange Rates Pipeline

## Setup
Run the following command to create a virtual environment and install required packages:

```bash
python3 -m venv venv && . venv/bin/activate
make install
```

## Run the pipeline
We can now proceed to run our pipeline:
```bash
make pipeline
```

If everything runs successfully, there should be a new column `converted_amount_eur` in the orders table.


## Potential Enhancements
- Adding unit and integration test to validate the logic
- Add a schedule to run daily or as required for the use case
- Automate pipeline with a tool, e.g., Dagster, Airflow, JenkinsBI, etc.
