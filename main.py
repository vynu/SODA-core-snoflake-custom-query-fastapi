"""
SODA Core Snowflake FastAPI Validation Server

A FastAPI application that validates custom Snowflake queries using SODA Core
data quality checks without subprocess calls.
"""

import asyncio
import gc
import logging
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from soda.scan import Scan

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SODA Core Snowflake Validator",
    description="Data quality validation for custom Snowflake queries using SODA Core",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models
class SnowflakeConfig(BaseModel):
    """Snowflake connection configuration"""
    account: str = Field(..., description="Snowflake account identifier")
    username: str = Field(..., description="Snowflake username")
    password: str = Field(..., description="Snowflake password")
    database: str = Field(..., description="Snowflake database name")
    warehouse: str = Field(..., description="Snowflake warehouse name")
    schema: str = Field(..., description="Snowflake schema name")
    role: Optional[str] = Field(default="PUBLIC", description="Snowflake role")
    connection_timeout: Optional[int] = Field(default=240, description="Connection timeout in seconds")


class ValidationRequest(BaseModel):
    """Request model for data validation"""
    snowflake_config: SnowflakeConfig
    custom_sql_query: Optional[str] = Field(default=None, description="Custom SQL query to validate (optional)")
    table_name: Optional[str] = Field(default=None, description="Table name for standard validation (optional)")
    validation_rules: str = Field(..., description="SODA CL validation rules in YAML format")
    scan_name: str = Field(default="api_validation", description="Name for the validation scan")


class CheckResult(BaseModel):
    """Individual validation check result"""
    name: str
    table: Optional[str]
    column: Optional[str]
    outcome: str
    value: Optional[Any]
    message: Optional[str]


class ValidationResponse(BaseModel):
    """Response model for validation results"""
    scan_id: str
    status: str
    exit_code: int
    data_quality_score: float
    passed_checks: int
    failed_checks: int
    warning_checks: int
    total_checks: int
    check_results: List[CheckResult]
    failed_rows_sample: Optional[List[Dict[str, Any]]] = None
    execution_time_seconds: Optional[float] = None
    logs: Optional[str] = None


class SODAValidationError(Exception):
    """Custom exception for SODA validation errors"""

    def __init__(self, error_type: str, message: str, details: Dict[str, Any] = None):
        self.error_type = error_type
        self.message = message
        self.details = details or {}
        super().__init__(message)


class ThreadSafeSODAService:
    """Thread-safe SODA Core validation service"""

    def __init__(self, max_workers: int = 4):
        self.scan_lock = threading.Lock()  # Critical for YAML emitter thread safety
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    @contextmanager
    def safe_scan_execution(self, scan_id: str):
        """Context manager ensuring thread-safe SODA scan execution"""
        with self.scan_lock:  # Prevents YAML emitter errors in concurrent execution
            try:
                scan = Scan()
                scan.set_verbose(True)
                scan.set_is_local(True)  # Prevent Soda Cloud integration overhead
                yield scan
            finally:
                # Clean up resources
                del scan
                gc.collect()

    def _build_snowflake_config_yaml(self, config: SnowflakeConfig, data_source_name: str) -> str:
        """Build Snowflake configuration YAML"""
        return f"""
data_source {data_source_name}:
  type: snowflake
  account: {config.account}
  username: {config.username}
  password: {config.password}
  database: {config.database}
  warehouse: {config.warehouse}
  schema: {config.schema}
  role: {config.role}
  connection_timeout: {config.connection_timeout}
  client_session_keep_alive: true
  session_parameters:
    QUERY_TAG: soda-data-quality-api
"""

    def _build_validation_rules(self, request: ValidationRequest) -> str:
        """Build validation rules for custom query or table"""
        if request.custom_sql_query:
            # For custom queries, wrap the query in parentheses for SODA
            return f"""
# Custom SQL query validation
checks for ({request.custom_sql_query}):
{request.validation_rules}
"""
        elif request.table_name:
            # For table validation
            return f"""
# Table validation
checks for {request.table_name}:
{request.validation_rules}
"""
        else:
            # Assume validation_rules contains the full check definition
            return request.validation_rules

    def _extract_results(self, scan: Scan, start_time: float, end_time: float) -> Dict[str, Any]:
        """Extract and process validation results"""
        scan_results = scan.get_scan_results()
        checks = scan_results.get('checks', [])

        # Count outcomes
        passed = len([c for c in checks if c.get('outcome') == 'pass'])
        failed = len([c for c in checks if c.get('outcome') == 'fail'])
        warnings = len([c for c in checks if c.get('outcome') == 'warn'])
        total = len(checks)

        # Calculate data quality score
        data_quality_score = passed / total if total > 0 else 0.0

        # Extract check results
        check_results = []
        failed_rows_sample = []

        for check in checks:
            check_result = CheckResult(
                name=check.get('name', check.get('definition', 'unnamed')),
                table=check.get('table'),
                column=check.get('column'),
                outcome=check.get('outcome', 'unknown'),
                value=check.get('checkValue'),
                message=check.get('message')
            )
            check_results.append(check_result)

            # Extract failed rows if available
            if check.get('outcome') in ['fail', 'warn']:
                diagnostics = check.get('diagnostics', {})
                for block in diagnostics.get('blocks', []):
                    if 'failedRows' in block and block['failedRows']:
                        failed_rows_sample.extend([
                            {
                                'check_name': check_result.name,
                                'table': check_result.table,
                                'failed_row': row
                            }
                            for row in block['failedRows'][:10]  # Limit to 10 samples per check
                        ])

        # Determine status
        if failed > 0:
            status = "failed"
        elif warnings > 0:
            status = "passed_with_warnings"
        else:
            status = "passed"

        return {
            'status': status,
            'exit_code': scan.execute() if hasattr(scan, '_exit_code') else (0 if failed == 0 else 2),
            'data_quality_score': data_quality_score,
            'passed_checks': passed,
            'failed_checks': failed,
            'warning_checks': warnings,
            'total_checks': total,
            'check_results': check_results,
            'failed_rows_sample': failed_rows_sample[:50],  # Limit total samples
            'execution_time_seconds': end_time - start_time,
            'logs': scan.get_logs_text()
        }

    async def execute_validation(self, request: ValidationRequest, scan_id: str) -> Dict[str, Any]:
        """Execute SODA validation with comprehensive error handling"""
        import time
        start_time = time.time()

        try:
            # Validate request
            if not request.custom_sql_query and not request.table_name:
                raise SODAValidationError(
                    "configuration_error",
                    "Either custom_sql_query or table_name must be provided"
                )

            # Execute validation in thread pool
            result = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self._execute_scan_sync,
                request,
                scan_id,
                start_time
            )

            return result

        except SODAValidationError:
            raise
        except asyncio.TimeoutError:
            raise SODAValidationError("timeout", "Validation execution timed out")
        except Exception as e:
            logger.error(f"Unexpected validation error: {str(e)}")
            raise SODAValidationError(
                "execution_error",
                f"Validation failed: {str(e)}",
                {"traceback": traceback.format_exc()}
            )

    def _execute_scan_sync(self, request: ValidationRequest, scan_id: str, start_time: float) -> Dict[str, Any]:
        """Synchronous scan execution (called from thread pool)"""
        data_source_name = "snowflake_api"

        with self.safe_scan_execution(scan_id) as scan:
            try:
                # Configure scan
                scan.set_data_source_name(data_source_name)
                scan.set_scan_definition_name(request.scan_name)

                # Add Snowflake configuration
                config_yaml = self._build_snowflake_config_yaml(request.snowflake_config, data_source_name)
                scan.add_configuration_yaml_str(config_yaml)

                # Add validation rules
                validation_rules = self._build_validation_rules(request)
                scan.add_sodacl_yaml_str(validation_rules)

                # Execute scan
                exit_code = scan.execute()
                end_time = time.time()

                # Extract results
                results = self._extract_results(scan, start_time, end_time)
                results['exit_code'] = exit_code

                return results

            except Exception as e:
                logger.error(f"Scan execution failed: {str(e)}")
                raise SODAValidationError("scan_execution_error", str(e))


# Initialize SODA service
soda_service = ThreadSafeSODAService()


# API endpoints
@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "SODA Core Snowflake Validator",
        "version": "1.0.0"
    }


@app.get("/validation-rules-examples")
async def get_validation_examples():
    """Get examples of validation rules"""
    examples = {
        "basic_validations": """  - row_count > 0
  - missing_count(email) = 0
  - duplicate_count(customer_id) = 0
  - invalid_percent(phone) < 5%""",

        "advanced_validations": """  - row_count between 1000 and 10000
  - freshness(created_date) < 1d
  - avg(order_amount) > 100
  - stddev(price) < 50""",

        "custom_metrics": """  - conversion_rate >= 0.15:
      conversion_rate query: |
        SELECT COUNT(CASE WHEN status = 'completed' THEN 1 END) * 1.0 / COUNT(*) 
        FROM sales_data
  - failed rows:
      fail query: |
        SELECT * FROM orders 
        WHERE ship_date < order_date"""
    }
    return examples


@app.post("/validate", response_model=ValidationResponse)
async def validate_data(
        request: ValidationRequest,
        background_tasks: BackgroundTasks
):
    """
    Execute SODA Core validation against Snowflake data

    This endpoint accepts either:
    1. A custom SQL query with validation rules
    2. A table name with validation rules

    Returns detailed validation results including data quality score,
    individual check outcomes, and sample failed rows.
    """
    scan_id = str(uuid.uuid4())
    logger.info(f"Starting validation scan {scan_id}")

    try:
        # Execute validation with timeout
        result = await asyncio.wait_for(
            soda_service.execute_validation(request, scan_id),
            timeout=300.0  # 5 minute timeout
        )

        # Log results in background
        background_tasks.add_task(log_validation_results, scan_id, result)

        response = ValidationResponse(
            scan_id=scan_id,
            **result
        )

        logger.info(f"Validation {scan_id} completed: {result['status']}")
        return response

    except asyncio.TimeoutError:
        logger.error(f"Validation {scan_id} timed out")
        raise HTTPException(
            status_code=408,
            detail="Validation request timed out after 5 minutes"
        )
    except SODAValidationError as e:
        logger.error(f"Validation {scan_id} failed: {e.message}")
        raise HTTPException(
            status_code=400 if e.error_type == "configuration_error" else 500,
            detail={"error_type": e.error_type, "message": e.message, "details": e.details}
        )
    except Exception as e:
        logger.error(f"Unexpected error in validation {scan_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


async def log_validation_results(scan_id: str, results: Dict[str, Any]):
    """Background task for logging validation results"""
    logger.info(
        f"Scan {scan_id} summary: "
        f"Quality Score: {results['data_quality_score']:.2%}, "
        f"Passed: {results['passed_checks']}, "
        f"Failed: {results['failed_checks']}, "
        f"Warnings: {results['warning_checks']}, "
        f"Duration: {results.get('execution_time_seconds', 0):.2f}s"
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
