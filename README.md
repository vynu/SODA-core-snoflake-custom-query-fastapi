# SODA Core Snowflake FastAPI Validator

A high-performance FastAPI application that validates custom Snowflake queries using SODA Core data quality checks. This application provides programmatic data quality validation without subprocess calls, supporting 25+ built-in validation rules and custom SQL query validation.

## Features

- 🚀 **FastAPI**: High-performance async web framework
- ❄️ **Snowflake Integration**: Direct connection to Snowflake data warehouse
- 🔍 **SODA Core**: Advanced data quality validation with 25+ built-in rules
- 🔧 **Custom SQL Support**: Validate results from complex multi-table joins
- 🛡️ **Thread-Safe**: Concurrent validation execution with proper locking
- 📊 **Detailed Results**: Comprehensive validation reports with failed row samples
- ⚡ **Optimized Performance**: Memory-efficient execution with connection pooling

## Requirements

- Python 3.11+
- UV package manager
- Snowflake account with appropriate permissions
- Valid Snowflake credentials (username/password or private key)

## Installation

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd soda-snowflake-validator
```

### 2. Install dependencies using UV

```bash
# Install UV if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv --python 3.11
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .

# For development (includes testing and linting tools)
uv pip install -e ".[dev]"

# For production deployment
uv pip install -e ".[production]"
```

### 3. Environment Setup

Create a `.env` file with your Snowflake credentials (optional, can also pass in API request):

```env
SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
```

## Usage

### Starting the Server

```bash
# Development mode with auto-reload
python main.py

# Or using uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Production mode
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

The API will be available at `http://localhost:8000` with interactive documentation at `http://localhost:8000/docs`.

### API Endpoints

#### 1. Health Check
```http
GET /
```

#### 2. Get Validation Rule Examples
```http
GET /validation-rules-examples
```

#### 3. Validate Data (Main Endpoint)
```http
POST /validate
```

## Example Usage

### Example 1: Basic Table Validation

**Request:**
```bash
curl -X POST "http://localhost:8000/validate" \
  -H "Content-Type: application/json" \
  -d '{
    "snowflake_config": {
      "account": "your_account.snowflakecomputing.com",
      "username": "your_username",
      "password": "your_password",
      "database": "SAMPLE_DB",
      "warehouse": "COMPUTE_WH",
      "schema": "PUBLIC",
      "role": "ANALYST"
    },
    "table_name": "CUSTOMERS",
    "validation_rules": "  - row_count > 0\n  - missing_count(email) = 0\n  - duplicate_count(customer_id) = 0",
    "scan_name": "customer_quality_check"
  }'
```

### Example 2: Custom SQL Query Validation

**Request:**
```bash
curl -X POST "http://localhost:8000/validate" \
  -H "Content-Type: application/json" \
  -d '{
    "snowflake_config": {
      "account": "your_account.snowflakecomputing.com", 
      "username": "your_username",
      "password": "your_password",
      "database": "ANALYTICS",
      "warehouse": "COMPUTE_WH",
      "schema": "PUBLIC"
    },
    "custom_sql_query": "SELECT o.order_id, o.customer_id, o.order_date, o.total_amount, c.customer_name FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.order_date >= DATEADD(day, -30, CURRENT_DATE)",
    "validation_rules": "  - row_count between 100 and 10000\n  - missing_count(customer_name) = 0\n  - avg(total_amount) > 50\n  - duplicate_count(order_id) = 0",
    "scan_name": "recent_orders_quality"
  }'
```

### Example 3: Advanced Custom Metrics

**Request:**
```bash
curl -X POST "http://localhost:8000/validate" \
  -H "Content-Type: application/json" \
  -d '{
    "snowflake_config": {
      "account": "your_account.snowflakecomputing.com",
      "username": "your_username", 
      "password": "your_password",
      "database": "SALES_DB",
      "warehouse": "ANALYTICS_WH",
      "schema": "METRICS"
    },
    "custom_sql_query": "SELECT product_id, category, price, discount, sales_count FROM product_sales WHERE created_date >= CURRENT_DATE - 7",
    "validation_rules": "  - conversion_rate >= 0.15:\n      conversion_rate query: |\n        SELECT COUNT(CASE WHEN sales_count > 0 THEN 1 END) * 1.0 / COUNT(*) as conversion_rate\n        FROM product_sales\n  - failed rows:\n      name: \"Invalid discount rules\"\n      fail query: |\n        SELECT product_id, category, price, discount\n        FROM product_sales\n        WHERE (category = 'PREMIUM' AND discount > 0.20)\n           OR (price > 1000 AND discount = 0)",
    "scan_name": "product_business_rules"
  }'
```

## Response Format

**Successful Response:**
```json
{
  "scan_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "passed",
  "exit_code": 0,
  "data_quality_score": 0.95,
  "passed_checks": 19,
  "failed_checks": 1,
  "warning_checks": 0,
  "total_checks": 20,
  "check_results": [
    {
      "name": "row_count > 0",
      "table": "CUSTOMERS",
      "column": null,
      "outcome": "pass",
      "value": 1500,
      "message": null
    },
    {
      "name": "missing_count(email) = 0",
      "table": "CUSTOMERS", 
      "column": "email",
      "outcome": "fail",
      "value": 5,
      "message": "5 missing values found in email column"
    }
  ],
  "failed_rows_sample": [
    {
      "check_name": "missing_count(email) = 0",
      "table": "CUSTOMERS",
      "failed_row": {
        "customer_id": 123,
        "name": "John Doe",
        "email": null
      }
    }
  ],
  "execution_time_seconds": 2.34,
  "logs": "INFO: Scan completed successfully..."
}
```

## Validation Rules Reference

### Built-in Metrics

#### Row-level Metrics
```yaml
- row_count > 1000                    # Minimum row count
- row_count between 1000 and 5000     # Row count range
```

#### Column Quality Metrics  
```yaml
- missing_count(email) = 0            # No missing values
- missing_percent(phone) < 5%         # Missing percentage threshold
- duplicate_count(customer_id) = 0    # No duplicates
- duplicate_percent(email) < 1%       # Duplicate percentage
```

#### Statistical Metrics
```yaml
- avg(order_amount) > 100             # Average value threshold
- min(price) >= 0                     # Minimum value validation
- max(quantity) <= 1000               # Maximum value validation
- sum(revenue) > 1000000              # Sum validation
- stddev(score) < 50                  # Standard deviation
```

#### Data Freshness
```yaml
- freshness(created_at) < 1d          # Data recency (1 day)
- freshness(updated_at) < 2h          # Recent updates (2 hours)
```

### Custom SQL Metrics

```yaml
- conversion_rate >= 0.15:
    conversion_rate query: |
      SELECT COUNT(CASE WHEN status = 'completed' THEN 1 END) * 1.0 / COUNT(*) 
      FROM sales_data

- avg_processing_time <= 24:
    avg_processing_time query: |
      SELECT AVG(EXTRACT(hours FROM completed_date - created_date))
      FROM order_processing
```

### Failed Rows Validation

```yaml
- failed rows:
    name: "Data integrity violations"
    fail query: |
      SELECT customer_id, order_id, order_date, ship_date
      FROM orders 
      WHERE ship_date < order_date
        OR order_date > CURRENT_DATE
```

## Architecture Overview

### Thread Safety
The application implements thread-safe SODA Core execution using:
- Mutex locks to prevent YAML emitter conflicts
- Connection pooling for database efficiency  
- Proper resource cleanup after each scan

### Error Handling
Comprehensive error handling covers:
- Snowflake connection failures
- Invalid SQL queries
- SODA Core validation errors
- Timeout management (5-minute default)
- Memory management for large datasets

### Performance Optimization
- Asynchronous execution with thread pools
- Limited failed row sampling (50 samples max)
- Connection keep-alive for long-running processes
- Memory cleanup after scan completion

## Development

### Running Tests

```bash
# Install development dependencies
uv pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=main --cov-report=html

# Run specific test
pytest tests/test_validation.py::test_basic_validation
```

### Code Formatting

```bash
# Format code
black .
isort .

# Lint code  
flake8 .
mypy .
```

### Adding New Validation Rules

1. **Built-in SODA Metrics**: Add to validation rules YAML
2. **Custom SQL Metrics**: Define using `query:` syntax
3. **Failed Rows Checks**: Use `fail query:` for detailed validation

Example custom metric:
```python
validation_rules = """
- custom_business_rule >= 0.8:
    custom_business_rule query: |
      SELECT 
        COUNT(CASE WHEN your_condition THEN 1 END) * 1.0 / COUNT(*) 
      FROM your_table
    fail: when < 0.5
    warn: when < 0.8
"""
```

## Deployment

### Production Deployment with Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install uv && uv pip install -e ".[production]"

EXPOSE 8000
CMD ["gunicorn", "main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000"]
```

### Environment Variables for Production

```env
# Snowflake Configuration (if using env vars instead of request body)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user  
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_db
SNOWFLAKE_WAREHOUSE=your_wh
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role

# Application Configuration
FASTAPI_ENV=production
LOG_LEVEL=INFO
MAX_WORKERS=4
REQUEST_TIMEOUT=300
```

## Troubleshooting

### Common Issues

**1. YAML Emitter Errors in Concurrent Execution**
- Ensure only one thread executes SODA scans simultaneously
- The application includes proper locking mechanisms

**2. Snowflake Connection Timeouts** 
```python
# Increase timeout in snowflake_config
"connection_timeout": 600  # 10 minutes
```

**3. Memory Issues with Large Datasets**
```yaml
# Limit failed row samples
- your_check:
    samples limit: 20
```

**4. Invalid SQL in Custom Queries**
- Test queries in Snowflake console first
- Ensure proper table/column references
- Check schema and database context

### Performance Tuning

**For Large Tables:**
- Use `LIMIT` in custom SQL queries for testing
- Implement sampling in validation rules
- Consider table partitioning strategies

**For High Concurrency:**
- Increase `max_workers` in ThreadPoolExecutor
- Monitor memory usage
- Implement request queueing if needed

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Make changes and add tests
4. Ensure tests pass (`pytest`)
5. Format code (`black . && isort .`)
6. Commit changes (`git commit -m 'Add amazing feature'`)
7. Push to branch (`git push origin feature/amazing-feature`)
8. Open Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Check the [troubleshooting section](#troubleshooting)
- Review [SODA Core documentation](https://docs.soda.io/)
- Open an issue on GitHub
- Check Snowflake connectivity and permissions

---

**Built with ❤️ using SODA Core, FastAPI, and Snowflake**