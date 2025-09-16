# Understanding main.py - SODA Core Snowflake Validator

This document explains how the FastAPI application works step-by-step, using a simple example that even entry-level coders can follow.

## What Does This Application Do?

The application takes a Snowflake SQL query and validation rules, then checks if the data meets quality standards. Think of it as an automated data inspector that tells you if your data is good or has problems.

## Simple Example: Checking Customer Data

Let's walk through a real example where we want to validate customer data from two tables joined together.

### Input: Sample API Request

```json
{
  "snowflake_config": {
    "account": "mycompany.snowflakecomputing.com",
    "username": "data_analyst",
    "password": "my_password", 
    "database": "SALES_DB",
    "warehouse": "COMPUTE_WH",
    "schema": "PUBLIC"
  },
  "custom_sql_query": "SELECT c.customer_id, c.customer_name, c.email, o.order_count FROM customers c JOIN (SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id) o ON c.customer_id = o.customer_id",
  "validation_rules": "  - row_count > 0\n  - missing_count(customer_name) = 0\n  - missing_count(email) = 0\n  - avg(order_count) >= 2",
  "scan_name": "customer_order_validation"
}
```

**What this does:**
- Joins customer table with order counts
- Checks that we have data (row_count > 0)
- Ensures no missing customer names or emails
- Verifies customers have at least 2 orders on average

## Step-by-Step Execution Flow

### Step 1: API Request Arrives
```
POST /validate
↓
FastAPI receives the JSON payload
↓ 
Pydantic validates the request structure
```

**What happens:**
- FastAPI checks if all required fields are present
- Creates a `ValidationRequest` object with proper data types
- If anything is missing or wrong, returns a 422 error immediately

### Step 2: Generate Unique Scan ID
```python
scan_id = str(uuid.uuid4())  # Example: "abc123-def456-789"
logger.info(f"Starting validation scan {scan_id}")
```

**What happens:**
- Creates a unique identifier for tracking this validation
- Logs the start of the process

### Step 3: Thread-Safe SODA Service Execution
```
ThreadSafeSODAService.execute_validation()
↓
Runs in background thread to avoid blocking other requests
↓
Uses mutex lock to prevent concurrent SODA executions
```

**Why this matters:**
- Multiple users can make requests simultaneously
- Each validation runs safely without interfering with others
- The app remains responsive while processing

### Step 4: Build Snowflake Configuration
```python
def _build_snowflake_config_yaml(self, config, data_source_name):
    return f"""
data_source snowflake_api:
  type: snowflake
  account: mycompany.snowflakecomputing.com
  username: data_analyst
  password: my_password
  database: SALES_DB
  warehouse: COMPUTE_WH
  schema: PUBLIC
  role: PUBLIC
  connection_timeout: 240
  client_session_keep_alive: true
"""
```

**What happens:**
- Converts the JSON config into YAML format that SODA Core understands
- Adds connection settings and timeouts
- Prepares the database connection configuration

#### Generated config_yaml File Structure

When the application processes our JSON input, it creates this YAML configuration internally:

```yaml
# This is what gets created from our JSON snowflake_config
data_source snowflake_api:
  type: snowflake
  account: mycompany.snowflakecomputing.com
  username: data_analyst
  password: my_password
  database: SALES_DB
  warehouse: COMPUTE_WH
  schema: PUBLIC
  role: PUBLIC
  connection_timeout: 240
  client_session_keep_alive: true
  session_parameters:
    QUERY_TAG: soda-data-quality-api
```

**YAML Structure Breakdown:**
- `data_source snowflake_api:` - Names our data source for SODA to reference
- `type: snowflake` - Tells SODA this is a Snowflake database
- `account:` through `schema:` - Your Snowflake connection details
- `connection_timeout: 240` - Wait up to 4 minutes for connection
- `client_session_keep_alive: true` - Keeps connection open for better performance
- `session_parameters:` - Additional Snowflake session settings

### Step 5: Build Validation Rules
```python
def _build_validation_rules(self, request):
    return f"""
# Custom SQL query validation
checks for (SELECT c.customer_id, c.customer_name, c.email, o.order_count FROM customers c JOIN (SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id) o ON c.customer_id = o.customer_id):
  - row_count > 0
  - missing_count(customer_name) = 0
  - missing_count(email) = 0
  - avg(order_count) >= 2
"""
```

**What happens:**
- Wraps the custom SQL query in SODA check format
- Adds the validation rules we specified
- Creates the complete validation definition

#### Generated validation_rules YAML File Structure

From our JSON input validation rules, the application creates this YAML structure:

```yaml
# This is the complete validation YAML that SODA Core uses
checks for (SELECT c.customer_id, c.customer_name, c.email, o.order_count FROM customers c JOIN (SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id) o ON c.customer_id = o.customer_id):
  - row_count > 0
  - missing_count(customer_name) = 0
  - missing_count(email) = 0
  - avg(order_count) >= 2
```

**YAML Structure Breakdown:**
- `checks for (SQL_QUERY):` - Tells SODA to run checks on our custom SQL query results
- `- row_count > 0` - Ensures we got some data back (not empty)
- `- missing_count(customer_name) = 0` - No null/empty customer names allowed
- `- missing_count(email) = 0` - No null/empty emails allowed  
- `- avg(order_count) >= 2` - Average orders per customer should be 2 or more

#### Alternative: Table-Based Validation YAML

If you used `table_name` instead of `custom_sql_query`, the YAML would look like this:

```yaml
# For table validation (when using table_name: "CUSTOMERS")
checks for CUSTOMERS:
  - row_count > 0
  - missing_count(customer_name) = 0
  - missing_count(email) = 0
  - duplicate_count(customer_id) = 0
```

#### More Complex Validation YAML Examples

The application can also generate more advanced validation structures:

```yaml
# Example: Advanced business rules validation
checks for (SELECT * FROM sales_data WHERE date >= CURRENT_DATE - 7):
  # Basic checks
  - row_count between 100 and 10000
  - missing_count(product_id) = 0
  
  # Custom business metric
  - profit_margin_compliance >= 0.95:
      profit_margin_compliance query: |
        SELECT COUNT(CASE WHEN profit_margin >= 0.10 THEN 1 END) * 1.0 / COUNT(*) 
        FROM sales_data
        WHERE product_category = 'ELECTRONICS'
  
  # Failed rows analysis  
  - failed rows:
      name: "Invalid pricing data"
      fail query: |
        SELECT product_id, price, cost, profit_margin
        FROM sales_data
        WHERE cost > price OR profit_margin < 0
```

**Advanced YAML Features:**
- `between 100 and 10000` - Range validation
- `custom_metric query:` - Define your own business logic calculations
- `failed rows:` - Get specific records that failed validation
- `name:` - Give descriptive names to your checks

### Step 6: SODA Core Execution
```
scan = Scan()
↓
scan.add_configuration_yaml_str(config_yaml)
↓
scan.add_sodacl_yaml_str(validation_rules)
↓
exit_code = scan.execute()
```

**What happens inside SODA:**
1. Connects to Snowflake using our credentials
2. Runs our custom SQL query
3. Applies each validation rule to the query results
4. Collects pass/fail results for each check

#### How the Two YAML Files Work Together

When SODA Core executes, it uses both YAML structures we created:

**1. Configuration YAML** (tells SODA HOW to connect):
```yaml
data_source snowflake_api:
  type: snowflake
  account: mycompany.snowflakecomputing.com
  # ... connection details
```

**2. Validation Rules YAML** (tells SODA WHAT to check):
```yaml
checks for (SELECT c.customer_id, c.customer_name, c.email, o.order_count FROM...):
  - row_count > 0
  - missing_count(customer_name) = 0
  # ... validation rules
```

**Execution Flow Inside SODA:**
1. **Connect**: Use config YAML to connect to Snowflake
2. **Query**: Execute the SQL from validation rules YAML  
3. **Analyze**: Apply each validation rule to the query results
4. **Report**: Return pass/fail status for each rule

**Example Internal Process:**
```
SODA connects to: mycompany.snowflakecomputing.com/SALES_DB/PUBLIC
↓
SODA runs: SELECT c.customer_id, c.customer_name, c.email, o.order_count FROM customers c JOIN...
↓
Query returns: 1,500 rows of customer data
↓
SODA checks: row_count > 0 → PASS (1,500 > 0)
SODA checks: missing_count(customer_name) = 0 → PASS (0 missing names)
SODA checks: missing_count(email) = 0 → FAIL (25 missing emails)  
SODA checks: avg(order_count) >= 2 → PASS (average is 3.2)
↓
SODA returns: 3 passed, 1 failed, overall score 75%
```

### Step 7: Process Results
```python
def _extract_results(self, scan, start_time, end_time):
    scan_results = scan.get_scan_results()
    checks = scan_results.get('checks', [])
    
    # Count outcomes
    passed = len([c for c in checks if c.get('outcome') == 'pass'])
    failed = len([c for c in checks if c.get('outcome') == 'fail'])
    
    # Calculate data quality score
    data_quality_score = passed / total if total > 0 else 0.0
```

**What happens:**
- Extracts individual check results from SODA
- Counts how many passed vs failed
- Calculates an overall data quality score (percentage)
- Collects any failed row samples for debugging

## Sample Output: What You Get Back

### Successful Validation Response
```json
{
  "scan_id": "abc123-def456-789",
  "status": "passed",
  "exit_code": 0,
  "data_quality_score": 1.0,
  "passed_checks": 4,
  "failed_checks": 0,
  "warning_checks": 0,
  "total_checks": 4,
  "check_results": [
    {
      "name": "row_count > 0",
      "table": null,
      "column": null,
      "outcome": "pass",
      "value": 1500,
      "message": null
    },
    {
      "name": "missing_count(customer_name) = 0",
      "table": null,
      "column": "customer_name",
      "outcome": "pass", 
      "value": 0,
      "message": null
    },
    {
      "name": "missing_count(email) = 0",
      "table": null,
      "column": "email",
      "outcome": "pass",
      "value": 0,
      "message": null
    },
    {
      "name": "avg(order_count) >= 2",
      "table": null,
      "column": "order_count",
      "outcome": "pass",
      "value": 3.2,
      "message": null
    }
  ],
  "failed_rows_sample": [],
  "execution_time_seconds": 2.34,
  "logs": "INFO: Scan completed successfully..."
}
```

### Failed Validation Response
```json
{
  "scan_id": "xyz789-abc123-456",
  "status": "failed",
  "exit_code": 2,
  "data_quality_score": 0.75,
  "passed_checks": 3,
  "failed_checks": 1,
  "warning_checks": 0,
  "total_checks": 4,
  "check_results": [
    {
      "name": "row_count > 0",
      "table": null,
      "column": null,
      "outcome": "pass",
      "value": 1500,
      "message": null
    },
    {
      "name": "missing_count(customer_name) = 0",
      "table": null,
      "column": "customer_name",
      "outcome": "pass",
      "value": 0,
      "message": null
    },
    {
      "name": "missing_count(email) = 0",
      "table": null,
      "column": "email",
      "outcome": "fail",
      "value": 25,
      "message": "Found 25 missing email addresses"
    },
    {
      "name": "avg(order_count) >= 2",
      "table": null,
      "column": "order_count", 
      "outcome": "pass",
      "value": 3.2,
      "message": null
    }
  ],
  "failed_rows_sample": [
    {
      "check_name": "missing_count(email) = 0",
      "table": null,
      "failed_row": {
        "customer_id": 123,
        "customer_name": "John Doe",
        "email": null,
        "order_count": 5
      }
    }
  ],
  "execution_time_seconds": 2.89,
  "logs": "WARN: 1 checks failed..."
}
```

## Understanding the Output

### Key Fields Explained

| Field | Meaning | Example |
|-------|---------|---------|
| `scan_id` | Unique identifier for this validation run | "abc123-def456" |
| `status` | Overall result: "passed", "failed", or "passed_with_warnings" | "passed" |
| `data_quality_score` | Percentage of checks that passed (0.0 to 1.0) | 0.75 = 75% passed |
| `passed_checks` | Number of validation rules that passed | 3 |
| `failed_checks` | Number of validation rules that failed | 1 |
| `check_results` | Detailed results for each validation rule | Array of individual results |
| `failed_rows_sample` | Examples of data rows that failed validation | Up to 50 sample rows |

### Check Result Details

Each validation rule returns:
- **name**: The validation rule that was tested
- **outcome**: "pass", "fail", or "warn"
- **value**: The actual measured value (e.g., row count = 1500)
- **message**: Human-readable explanation of the result

## Error Handling Flow

### If Something Goes Wrong

```
Error occurs during execution
↓
Custom SODAValidationError is raised
↓
FastAPI catches the error
↓
Returns appropriate HTTP status code with error details
```

**Common Error Types:**
- **400 Bad Request**: Invalid configuration (missing required fields)
- **500 Internal Server Error**: Database connection issues, SQL errors
- **408 Timeout**: Validation took longer than 5 minutes

### Example Error Response
```json
{
  "detail": {
    "error_type": "scan_execution_error",
    "message": "Failed to connect to Snowflake: Invalid credentials",
    "details": {
      "connection_timeout": true
    }
  }
}
```

## Thread Safety: Why It Matters

The application uses a **ThreadSafeSODAService** with mutex locks because:

1. **SODA Core Issue**: The underlying SODA library has thread-safety issues with YAML processing
2. **Solution**: Only one SODA scan can run at a time, but multiple requests can be queued
3. **User Impact**: Multiple users can still make requests simultaneously without errors

```python
with self.scan_lock:  # Only one SODA scan at a time
    scan = Scan()
    # ... safe execution here
```

## Performance Considerations

### Memory Management
- Each scan cleans up resources after completion
- Failed row samples are limited to 50 records total
- Connection pooling reduces database overhead

### Execution Time
- Simple queries: 1-3 seconds
- Complex queries with joins: 3-10 seconds  
- Large datasets: 10-60 seconds
- Timeout after 5 minutes

## Summary

The application follows this simple pattern:

1. **Receive** validation request
2. **Connect** to Snowflake 
3. **Run** your SQL query
4. **Apply** data quality checks
5. **Return** detailed results

This makes it easy to catch data quality issues early and ensure your business logic rules are being followed across your data pipeline.