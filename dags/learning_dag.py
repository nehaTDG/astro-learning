from datetime import datetime, timedelta
from logging import Logger
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

# =============================================================================
# CONSTANTS AND CONFIGURATION
# =============================================================================

DAG_ID = "sample_etl_pipeline_v1"
DAG_OWNER = "data_team"
DAG_DESCRIPTION = "Sample ETL pipeline that extracts, transforms, and loads data"

# Default arguments for all tasks in this DAG
DEFAULT_ARGS = {
    "owner": DAG_OWNER,
    "retries": 2,  # Retry tasks up to 2 times on failure
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
    "start_date": datetime(2024, 1, 1),  # When the DAG starts being scheduled
    "email_on_failure": False,  # Set to True if you want email alerts
    "email_on_retry": False,
}

# =============================================================================
# DAG DEFINITION
# =============================================================================

@DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DAG_DESCRIPTION,
    schedule="0 2 * * *",  # Run daily at 2:00 AM UTC (cron format)
    catchup=False,  # Don't backfill past DAG runs
    tags=["etl", "sample", "production"],  # Tags for organizing DAGs in UI
    doc_md=__doc__,  # Use the module docstring as DAG documentation
)
def sample_etl_pipeline():
    """
    Main DAG function that defines all tasks and their dependencies.
    Using TaskFlow API (decorators) for cleaner, more readable code.
    """

    # =========================================================================
    # TASK 1: EXTRACT - Simulating data extraction from a source
    # =========================================================================
    
    @task(task_id="extract_data")
    def extract_data(logger: Logger):
        """
        Extract data from a source system (database, API, file, etc).
        
        In a real scenario, this would:
        - Connect to your data source
        - Query or fetch data
        - Return the data for downstream tasks
        """
        logger.info("Starting data extraction")
        
        # Simulate extracting data
        sample_data = [
            {"id": 1, "name": "Alice", "amount": 100},
            {"id": 2, "name": "Bob", "amount": 200},
            {"id": 3, "name": "Charlie", "amount": 150},
        ]
        
        logger.info(f"Successfully extracted {len(sample_data)} records")
        
        # Return data to be used by downstream tasks
        return sample_data

    # =========================================================================
    # TASK 2: VALIDATE - Check if extracted data is valid
    # =========================================================================
    
    @task(task_id="validate_data")
    def validate_data(data, logger: Logger):
        """
        Validate the extracted data to ensure quality.
        
        Checks:
        - Data is not empty
        - Required fields exist
        - Data types are correct
        """
        logger.info("Starting data validation")
        
        if not data:
            raise ValueError("Extracted data is empty!")
        
        # Validate each record
        required_fields = ["id", "name", "amount"]
        for record in data:
            for field in required_fields:
                if field not in record:
                    raise ValueError(f"Record missing required field: {field}")
        
        logger.info(f"Validation passed for {len(data)} records")
        return data

    # =========================================================================
    # TASK 3: TRANSFORM - Process and enrich the data
    # =========================================================================
    
    @task(task_id="transform_data")
    def transform_data(data, logger: Logger):
        """
        Transform the data (clean, enrich, calculate metrics, etc).
        
        Operations:
        - Add a timestamp to each record
        - Calculate derived fields
        - Standardize formats
        """
        logger.info("Starting data transformation")
        
        transformed_data = []
        for record in data:
            # Add transformation logic
            transformed_record = {
                **record,
                "processed_at": datetime.utcnow().isoformat(),
                "amount_with_tax": record["amount"] * 1.1,  # Add 10% tax
                "category": "high_value" if record["amount"] > 150 else "standard",
            }
            transformed_data.append(transformed_record)
        
        logger.info(f"Transformation completed for {len(transformed_data)} records")
        return transformed_data

    # =========================================================================
    # TASK 4: LOAD - Save the processed data to destination
    # =========================================================================
    
    @task(task_id="load_data")
    def load_data(data, logger: Logger):
        """
        Load the transformed data to a destination (database, data warehouse, file, etc).
        
        In a real scenario, this would:
        - Connect to target database/system
        - Insert or update records
        - Handle duplicates
        """
        logger.info("Starting data load")
        
        # Simulate loading data
        records_loaded = len(data)
        
        logger.info(f"Successfully loaded {records_loaded} records to destination")
        
        # Return summary for monitoring
        return {
            "status": "success",
            "records_loaded": records_loaded,
            "timestamp": datetime.utcnow().isoformat(),
        }

    # =========================================================================
    # TASK 5: SUMMARY - Generate completion summary
    # =========================================================================
    
    @task(
        task_id="generate_summary",
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,  # Run unless tasks failed
    )
    def generate_summary(load_result, logger: Logger):
        """
        Generate a summary of the pipeline execution.
        Useful for monitoring and alerting.
        """
        logger.info("=" * 60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Status: {load_result['status']}")
        logger.info(f"Records Processed: {load_result['records_loaded']}")
        logger.info(f"Completion Time: {load_result['timestamp']}")
        logger.info("=" * 60)
        
        return {
            "pipeline_status": "completed",
            "details": load_result,
        }

    # =========================================================================
    # DEFINE TASK DEPENDENCIES (DAG STRUCTURE)
    # =========================================================================
    
    # Extract -> Validate -> Transform -> Load -> Summary
    extracted = extract_data()
    validated = validate_data(extracted)
    transformed = transform_data(validated)
    loaded = load_data(transformed)
    generate_summary(loaded)


# =============================================================================
# INSTANTIATE THE DAG
# =============================================================================

# This call registers the DAG with Airflow
dag = sample_etl_pipeline()



