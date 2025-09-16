# SODA-core-snoflake-custom-query-fastapi

# SODA Core Snowflake Integration Guide

SODA Core provides robust programmatic capabilities for data quality validation with Snowflake, offering **25+ built-in validation rules** and complete support for custom SQL query validation. This comprehensive guide covers everything needed to build a FastAPI application that validates custom Snowflake queries using pure Python SODA Core APIs.

## Installation and dependencies

SODA Core requires **Python 3.8-3.10** with pip 21.0 or greater. Install the Snowflake-specific package:

```bash
# Basic open-source version
pip install soda-core-snowflake

# Extended version with Soda Cloud integration (45-day free trial)
pip install -i https://pypi.cloud.soda.io soda-snowflake
```

**Key dependencies automatically installed:**
- `snowflake-connector-python` for Snowflake connectivity
- `cryptography` for private key authentication
- `PyYAML` for configuration parsing
- Core SODA libraries for data quality testing

**Critical requirement**: Use virtual environments to avoid dependency conflicts, as SODA Core has specific version constraints that may conflict with other packages.

## Core Python API architecture

### Basic scan initialization pattern

```python
from soda.scan import Scan

# Create scan instance
scan = Scan()
scan.set_data_source_name("snowflake_datasource")
scan.set_scan_definition_name("my_quality_check")

# Add configuration programmatically
scan.add_configuration_yaml_str("""
data_source snowflake_datasource:
  type: snowflake
  account: ${SNOWFLAKE_ACCOUNT}
  username: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  database: ANALYTICS
  warehouse: COMPUTE_WH
  schema: PUBLIC
  role: DATA_ANALYST
""")

# Define validation rules
scan.add_sodacl_yaml_str("""
checks for CUSTOMERS:
  - row_count > 0
  - missing_count(email) = 0
  - duplicate_count(customer_id) = 0
""")

# Execute and handle results
exit_code = scan.execute()
results = scan.get_scan_results()
```

### Essential API methods for programmatic usage

**Configuration Methods:**
- `scan.add_configuration_yaml_str(yaml_string)` - Inline configuration
- `scan.add_configuration_yaml_file(file_path)` - External config file
- `scan.add_variables({"key": "value"})` - Dynamic variable substitution

**Check Management:**
- `scan.add_sodacl_yaml_str(checks_yaml)` - Define validation rules inline
- `scan.add_sodacl_yaml_file(file_path)` - Load checks from file
- `scan.add_sodacl_yaml_files(directory_path)` - Load multiple check files

**Execution and Results:**
- `scan.execute()` - Run validation (returns exit codes 0-4)
- `scan.get_scan_results()` - Complete results dictionary
- `scan.has_check_fails()` - Boolean check for failures
- `scan.get_checks_fail_text()` - Failed check details

## Snowflake connection configuration

### Authentication methods supported

**Username/Password Authentication:**
```yaml
data_source snowflake_prod:
  type: snowflake
  account: ${SNOWFLAKE_ACCOUNT}
  username: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  database: ANALYTICS
  warehouse: COMPUTE_WH
  schema: PUBLIC
  role: DATA_ANALYST
  connection_timeout: 240
  client_session_keep_alive: true
```

**Private Key Authentication (Most Secure):**
```yaml
data_source snowflake_secure:
  type: snowflake
  account: ${SNOWFLAKE_ACCOUNT}
  username: ${SNOWFLAKE_USER}
  authenticator: SNOWFLAKE_JWT
  private_key: |
    -----BEGIN ENCRYPTED PRIVATE KEY-----
    -----END ENCRYPTED PRIVATE KEY-----
  private_key_passphrase: ${SNOWFLAKE_PASSPHRASE}
  database: ANALYTICS
  warehouse: COMPUTE_WH
  schema: PUBLIC
```

**External Browser SSO:**
```yaml
data_source snowflake_sso:
  type: snowflake
  account: ${SNOWFLAKE_ACCOUNT}
  authenticator: externalbrowser
  database: ANALYTICS
  warehouse: COMPUTE_WH
  schema: PUBLIC
```

**OAuth 2.0 Client Credentials:**
```yaml
data_source snowflake_oauth:
  type: snowflake
  authenticator: OAUTH_CLIENT_CREDENTIALS
  oauth_client_id: ${OAUTH_CLIENT_ID}
  oauth_client_secret: ${OAUTH_CLIENT_SECRET}
  oauth_token_request_url: https://idp.company.com/oauth/token
```

### Advanced connection parameters

```yaml
# Session and proxy settings
session_parameters:
  QUERY_TAG: soda-data-quality
  QUOTED_IDENTIFIERS_IGNORE_CASE: false
proxy_http: http://proxy:8080
proxy_https: https://proxy:8080
```

## Custom SQL query validation approaches

SODA Core offers **multiple methods** for validating custom SQL queries, providing flexibility for complex business logic validation.

### User-defined checks with custom SQL queries

```python
custom_sql_checks = """
checks for sales_data:
  # Custom metric using SQL query
  - conversion_rate >= 0.15:
      conversion_rate query: |
        SELECT 
          COUNT(CASE WHEN status = 'completed' THEN 1 END) * 1.0 / 
          COUNT(*) as conversion_rate
        FROM sales_data
      fail: when < 0.10
      warn: when < 0.15

  # Complex business rule validation
  - vip_discount_compliance > 0.95:
      vip_discount_compliance query: |
        SELECT 
          COUNT(CASE WHEN customer_type = 'VIP' AND discount >= 0.15 THEN 1 END) * 1.0 /
          COUNT(CASE WHEN customer_type = 'VIP' THEN 1 END)
        FROM sales_data
        WHERE customer_type = 'VIP'
"""

scan.add_sodacl_yaml_str(custom_sql_checks)
```

### Failed rows checks for detailed validation

```python
failed_rows_checks = """
checks for orders:
  # Business logic validation with specific failing records
  - failed rows:
      name: "Invalid order dates"
      fail query: |
        SELECT order_id, order_date, ship_date, customer_id
        FROM orders 
        WHERE ship_date < order_date
        
  - failed rows:
      name: "Pricing rule violations" 
      fail query: |
        SELECT product_id, category, price, discount
        FROM products
        WHERE (category = 'PREMIUM' AND discount > 0.20)
           OR (category = 'BASIC' AND price > 1000 AND discount = 0)
"""
```

### Expression-based custom metrics

```python
expression_checks = """
checks for customer_orders:
  # Statistical validation using expressions
  - avg_order_processing_days <= 3:
      avg_order_processing_days expression: |
        AVG(EXTRACT(days FROM completed_date - created_date))
      
  - order_value_consistency between 0.95 and 1.05:
      order_value_consistency expression: |
        AVG(total_amount) / AVG(item_total + tax_amount + shipping_amount)
"""
```

## Available validation rules comprehensive reference

### Built-in metric categories

**Standard Numeric Metrics:**
- `row_count` - Total record count validation
- `sum(column)`, `avg(column)`, `min(column)`, `max(column)` - Aggregation metrics
- `stddev(column)`, `variance(column)` - Statistical metrics
- `percentile(column, 0.95)` - Percentile-based validation

**Data Quality Metrics:**
- `missing_count(column)`, `missing_percent(column)` - Null value detection
- `duplicate_count(column)`, `duplicate_percent(column)` - Duplicate identification
- `invalid_count(column)`, `invalid_percent(column)` - Format/range validation
- `valid_count(column)`, `valid_percent(column)` - Positive validation

**Advanced Validation Types:**
- `freshness(timestamp_column) < 1d` - Data recency validation
- `schema` - Column existence and data type validation
- `values in (column) must exist in other_table (other_column)` - Referential integrity
- `reconciliation` - Cross-dataset comparison for migrations

### Validation rule syntax patterns

**Threshold Configurations:**
```python
validation_patterns = """
checks for data_table:
  # Fixed thresholds
  - row_count > 1000
  - missing_count(email) = 0
  
  # Boundary thresholds  
  - row_count between 1000 and 10000
  - invalid_percent(phone) < 5%
  
  # Multiple alert levels
  - missing_count(critical_field) = 0:
      warn: when > 0
      fail: when > 100
      name: "Critical field completeness"
      
  # Custom sampling configuration
  - duplicate_count(id) = 0:
      samples limit: 100
      collect failed rows: true
"""
```

## FastAPI application implementation

### Complete FastAPI integration example

```python
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from soda.scan import Scan
import asyncio
import logging
import uuid
from typing import Dict, Any, Optional

app = FastAPI()

class SnowflakeValidationRequest(BaseModel):
    snowflake_config: Dict[str, str]
    custom_sql_query: Optional[str] = None
    table_name: Optional[str] = None
    validation_rules: str
    scan_name: str = "api_validation"

class ValidationResponse(BaseModel):
    scan_id: str
    status: str
    exit_code: int
    passed_checks: int
    failed_checks: int
    warnings: int
    results: Dict[str, Any]
    failed_rows: Optional[list] = None

@app.post("/validate-snowflake-data", response_model=ValidationResponse)
async def validate_snowflake_data(
    request: SnowflakeValidationRequest,
    background_tasks: BackgroundTasks
):
    """
    Execute SODA Core validation against Snowflake with custom queries
    """
    scan_id = str(uuid.uuid4())
    
    try:
        # Execute validation asynchronously
        result = await asyncio.get_event_loop().run_in_executor(
            None, execute_snowflake_validation, request, scan_id
        )
        
        # Log results in background
        background_tasks.add_task(log_validation_results, scan_id, result)
        
        return ValidationResponse(
            scan_id=scan_id,
            status="completed",
            exit_code=result["exit_code"],
            passed_checks=result["passed_checks"],
            failed_checks=result["failed_checks"], 
            warnings=result["warnings"],
            results=result["scan_results"],
            failed_rows=result.get("failed_rows", [])
        )
        
    except Exception as e:
        logging.error(f"Validation failed for scan {scan_id}: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Validation execution failed: {str(e)}"
        )

def execute_snowflake_validation(
    request: SnowflakeValidationRequest, 
    scan_id: str
) -> Dict[str, Any]:
    """
    Synchronous SODA scan execution (called from thread pool)
    """
    scan = Scan()
    scan.set_data_source_name("snowflake_api")
    scan.set_scan_definition_name(request.scan_name)
    scan.set_verbose(True)
    
    # Configure Snowflake connection
    config_yaml = f"""
data_source snowflake_api:
  type: snowflake
  account: {request.snowflake_config['account']}
  username: {request.snowflake_config['username']}
  password: {request.snowflake_config['password']}
  database: {request.snowflake_config['database']}
  warehouse: {request.snowflake_config['warehouse']}
  schema: {request.snowflake_config['schema']}
  role: {request.snowflake_config.get('role', 'PUBLIC')}
  connection_timeout: 240
  client_session_keep_alive: true
"""
    
    scan.add_configuration_yaml_str(config_yaml)
    
    # Handle custom SQL query validation
    if request.custom_sql_query:
        # Create temporary view for custom query validation
        validation_rules = f"""
# Custom query validation
checks for ({request.custom_sql_query}):
{request.validation_rules}
"""
    else:
        # Standard table validation
        validation_rules = request.validation_rules
    
    scan.add_sodacl_yaml_str(validation_rules)
    
    # Execute scan
    exit_code = scan.execute()
    
    # Process results
    scan_results = scan.get_scan_results()
    failed_rows = []
    
    # Extract failed rows if available
    for check in scan_results.get('checks', []):
        if check.get('outcome') in ['fail', 'warn']:
            diagnostics = check.get('diagnostics', {})
            for block in diagnostics.get('blocks', []):
                if 'failedRowsQuery' in block:
                    failed_rows.extend(block.get('failedRows', []))
    
    return {
        'exit_code': exit_code,
        'passed_checks': len([c for c in scan_results.get('checks', []) if c.get('outcome') == 'pass']),
        'failed_checks': len([c for c in scan_results.get('checks', []) if c.get('outcome') == 'fail']),
        'warnings': len([c for c in scan_results.get('checks', []) if c.get('outcome') == 'warn']),
        'scan_results': scan_results,
        'failed_rows': failed_rows,
        'logs': scan.get_logs_text()
    }

async def log_validation_results(scan_id: str, results: Dict[str, Any]):
    """Background task for logging validation results"""
    logging.info(f"Scan {scan_id} completed with exit code {results['exit_code']}")
```

### Advanced error handling and thread safety

```python
import threading
from contextlib import contextmanager

class ThreadSafeSODAService:
    def __init__(self):
        self.scan_lock = threading.Lock()  # Critical for YAML emitter thread safety
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    @contextmanager
    def safe_scan_execution(self, scan_id: str):
        """Context manager ensuring thread-safe SODA scan execution"""
        with self.scan_lock:  # Prevents YAML emitter errors in concurrent execution
            try:
                scan = Scan()
                yield scan
            finally:
                # Clean up resources
                del scan
                gc.collect()
    
    async def execute_validation_safely(self, config: dict) -> dict:
        """Execute SODA validation with comprehensive error handling"""
        scan_id = str(uuid.uuid4())
        
        try:
            with self.safe_scan_execution(scan_id) as scan:
                # Configure scan
                scan.set_data_source_name(config['data_source'])
                scan.add_configuration_yaml_str(config['snowflake_config'])
                scan.add_sodacl_yaml_str(config['validation_rules'])
                
                # Execute with timeout
                exit_code = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, scan.execute),
                    timeout=300
                )
                
                # Handle different exit codes
                if exit_code == 0:
                    status = "all_passed"
                elif exit_code == 1:
                    status = "warnings"  
                elif exit_code == 2:
                    status = "failures"
                else:
                    status = "error"
                
                return {
                    'success': True,
                    'status': status,
                    'exit_code': exit_code,
                    'results': scan.get_scan_results(),
                    'has_failures': scan.has_check_fails(),
                    'logs': scan.get_logs_text()
                }
                
        except asyncio.TimeoutError:
            return {
                'success': False,
                'error': 'timeout',
                'message': 'Validation timed out after 300 seconds'
            }
        except Exception as e:
            logging.error(f"Validation failed: {str(e)}")
            return {
                'success': False,
                'error': 'execution_failure',
                'message': str(e)
            }

# Usage in FastAPI endpoint
soda_service = ThreadSafeSODAService()

@app.post("/safe-validation")
async def safe_validation_endpoint(config: dict):
    result = await soda_service.execute_validation_safely(config)
    
    if not result['success']:
        raise HTTPException(status_code=500, detail=result['message'])
    
    return result
```

## Result parsing and validation outcome handling

### Comprehensive result processing

```python
def process_validation_results(scan: Scan) -> Dict[str, Any]:
    """Extract and categorize detailed validation results"""
    results = scan.get_scan_results()
    
    summary = {
        'total_checks': len(results.get('checks', [])),
        'passed': 0,
        'failed': 0,
        'warned': 0,
        'errors': 0,
        'check_details': [],
        'failed_row_samples': [],
        'metrics': {}
    }
    
    for check in results.get('checks', []):
        outcome = check.get('outcome', 'unknown')
        
        # Count outcomes
        if outcome == 'pass':
            summary['passed'] += 1
        elif outcome == 'fail':
            summary['failed'] += 1
        elif outcome == 'warn':
            summary['warned'] += 1
        else:
            summary['errors'] += 1
        
        # Extract check details
        check_detail = {
            'name': check.get('name', check.get('definition', 'unnamed')),
            'table': check.get('table'),
            'column': check.get('column'),
            'outcome': outcome,
            'value': check.get('checkValue'),
            'evaluation_time': check.get('evaluationTimestamp')
        }
        
        # Extract failed rows if available
        if outcome in ['fail', 'warn']:
            diagnostics = check.get('diagnostics', {})
            for block in diagnostics.get('blocks', []):
                if 'failedRows' in block:
                    summary['failed_row_samples'].extend({
                        'check': check_detail['name'],
                        'table': check_detail['table'],
                        'rows': block['failedRows'],
                        'query': block.get('failedRowsQuery', '')
                    })
        
        summary['check_details'].append(check_detail)
    
    # Extract scan-level metrics
    summary['metrics'] = {
        'scan_start_time': results.get('scanStartTimestamp'),
        'scan_end_time': results.get('scanEndTimestamp'),
        'data_source': results.get('dataSource'),
        'soda_version': results.get('sodaVersion')
    }
    
    return summary
```

### Custom validation result handlers

```python
class ValidationResultHandler:
    """Custom handler for processing SODA validation results"""
    
    @staticmethod
    def extract_business_metrics(results: dict) -> dict:
        """Extract business-relevant metrics from validation results"""
        business_metrics = {
            'data_quality_score': 0.0,
            'critical_failures': 0,
            'completeness_score': 1.0,
            'consistency_score': 1.0
        }
        
        checks = results.get('checks', [])
        if not checks:
            return business_metrics
        
        # Calculate overall data quality score
        total_checks = len(checks)
        passed_checks = len([c for c in checks if c.get('outcome') == 'pass'])
        business_metrics['data_quality_score'] = passed_checks / total_checks if total_checks > 0 else 0
        
        # Count critical failures (failed checks with "critical" in name)
        business_metrics['critical_failures'] = len([
            c for c in checks 
            if c.get('outcome') == 'fail' and 'critical' in c.get('name', '').lower()
        ])
        
        # Calculate completeness score (inverse of missing data checks)
        completeness_checks = [c for c in checks if 'missing' in c.get('definition', '')]
        if completeness_checks:
            passed_completeness = len([c for c in completeness_checks if c.get('outcome') == 'pass'])
            business_metrics['completeness_score'] = passed_completeness / len(completeness_checks)
        
        return business_metrics
    
    @staticmethod
    def create_executive_summary(results: dict) -> str:
        """Generate executive summary of validation results"""
        metrics = ValidationResultHandler.extract_business_metrics(results)
        
        summary = f"""
Data Quality Assessment Summary:
- Overall Quality Score: {metrics['data_quality_score']:.1%}
- Critical Failures: {metrics['critical_failures']}
- Data Completeness: {metrics['completeness_score']:.1%}
- Data Consistency: {metrics['consistency_score']:.1%}

Status: {'HEALTHY' if metrics['data_quality_score'] > 0.95 else 'REQUIRES ATTENTION'}
        """
        
        return summary.strip()
```

## Best practices and production considerations

### Performance optimization strategies

**Connection Management:**
- Use connection pooling for database connections
- Implement connection timeouts (240 seconds recommended)
- Enable `client_session_keep_alive` for long-running processes

**Memory Management:**
- Set `scan.set_is_local(True)` to prevent Soda Cloud integration overhead
- Limit failed row samples with `samples limit: 50` in check definitions
- Use garbage collection after scan completion for large datasets

**Concurrency Handling:**
- **Critical**: Use threading locks to prevent YAML emitter errors in concurrent execution
- Limit concurrent scans (recommended: 4 maximum)
- Use thread pools for non-blocking execution in web applications

### Security best practices

```python
# Use environment variables for sensitive configuration
config_template = """
data_source snowflake_secure:
  type: snowflake
  account: ${SNOWFLAKE_ACCOUNT}
  username: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  database: ${SNOWFLAKE_DATABASE}
  warehouse: ${SNOWFLAKE_WAREHOUSE}
  schema: ${SNOWFLAKE_SCHEMA}
  role: ${SNOWFLAKE_ROLE}
  authenticator: SNOWFLAKE_JWT  # Prefer private key auth in production
  private_key_path: ${SNOWFLAKE_PRIVATE_KEY_PATH}
"""

# Never hardcode credentials in source code
# Use secure credential management systems (AWS Secrets Manager, Azure Key Vault, etc.)
```

### Error handling patterns

```python
class SODAValidationError(Exception):
    """Custom exception for SODA validation errors"""
    def __init__(self, error_type: str, message: str, details: dict = None):
        self.error_type = error_type
        self.message = message  
        self.details = details or {}
        super().__init__(message)

async def robust_validation_execution(config: dict) -> dict:
    """Robust validation with comprehensive error handling"""
    try:
        # Input validation
        required_fields = ['snowflake_config', 'validation_rules']
        if not all(field in config for field in required_fields):
            raise SODAValidationError(
                "configuration_error",
                "Missing required configuration fields",
                {"missing": [f for f in required_fields if f not in config]}
            )
        
        # Execute validation
        result = await execute_validation_safely(config)
        return result
        
    except SODAValidationError:
        raise  # Re-raise custom errors
    except asyncio.TimeoutError:
        raise SODAValidationError("timeout", "Validation execution timed out")
    except Exception as e:
        raise SODAValidationError("unexpected_error", str(e), {"traceback": traceback.format_exc()})
```

This comprehensive guide provides everything needed to implement robust, production-ready data quality validation using SODA Core with Snowflake in FastAPI applications. The programmatic approach eliminates subprocess dependencies while providing full access to SODA Core's extensive validation capabilities and custom SQL query support.