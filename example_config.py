"""
Example configurations and usage patterns for SODA Core Snowflake Validator

This file contains various examples of how to configure and use the validation service.
Copy and modify these examples for your specific use cases.
"""

# Example 1: Basic table validation configuration
BASIC_TABLE_VALIDATION = {
    "snowflake_config": {
        "account": "your_account.snowflakecomputing.com",
        "username": "your_username",
        "password": "your_password",
        "database": "SAMPLE_DATA",
        "warehouse": "COMPUTE_WH",
        "schema": "TPCH_SF1",
        "role": "ACCOUNTADMIN"
    },
    "table_name": "CUSTOMER",
    "validation_rules": """
  - row_count > 0
  - missing_count(C_NAME) = 0
  - duplicate_count(C_CUSTKEY) = 0
  - invalid_percent(C_PHONE) < 1%
""",
    "scan_name": "customer_data_quality"
}

# Example 2: Custom SQL query with joins validation
CUSTOM_QUERY_VALIDATION = {
    "snowflake_config": {
        "account": "your_account.snowflakecomputing.com",
        "username": "your_username",
        "password": "your_password",
        "database": "ANALYTICS_DB",
        "warehouse": "ANALYTICS_WH",
        "schema": "MARTS",
        "role": "DATA_ANALYST"
    },
    "custom_sql_query": """
                        SELECT o.O_ORDERKEY,
                               o.O_CUSTKEY,
                               o.O_ORDERDATE,
                               o.O_TOTALPRICE,
                               c.C_NAME,
                               c.C_MKTSEGMENT
                        FROM ORDERS o
                                 JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
                        WHERE o.O_ORDERDATE >= DATEADD(day, -30, CURRENT_DATE)
                          AND o.O_ORDERSTATUS = 'F'
                        """,
    "validation_rules": """
  - row_count between 100 and 50000
  - missing_count(C_NAME) = 0
  - missing_count(O_TOTALPRICE) = 0
  - avg(O_TOTALPRICE) > 1000
  - duplicate_count(O_ORDERKEY) = 0
  - invalid_percent(C_MKTSEGMENT) < 1%
""",
    "scan_name": "recent_completed_orders_quality"
}

# Example 3: Advanced validation with custom metrics
ADVANCED_CUSTOM_METRICS = {
    "snowflake_config": {
        "account": "your_account.snowflakecomputing.com",
        "username": "your_username",
        "password": "your_password",
        "database": "SALES_DW",
        "warehouse": "ANALYTICS_WH",
        "schema": "FACT_TABLES",
        "role": "DATA_ENGINEER"
    },
    "custom_sql_query": """
                        SELECT product_id,
                               category,
                               subcategory,
                               price,
                               cost,
                               profit_margin,
                               sales_count,
                               revenue,
                               created_date
                        FROM product_performance
                        WHERE created_date >= CURRENT_DATE - INTERVAL '7 days'
                        """,
    "validation_rules": """
  # Basic data quality checks
  - row_count > 50
  - missing_count(product_id) = 0
  - duplicate_count(product_id) = 0

  # Business rule validations
  - profit_margin_compliance >= 0.95:
      profit_margin_compliance query: |
        SELECT 
          COUNT(CASE WHEN profit_margin >= 0.10 THEN 1 END) * 1.0 / COUNT(*) 
        FROM product_performance
        WHERE category IN ('ELECTRONICS', 'CLOTHING')

  - high_value_product_ratio between 0.15 and 0.40:
      high_value_product_ratio query: |
        SELECT 
          COUNT(CASE WHEN price > 100 THEN 1 END) * 1.0 / COUNT(*) 
        FROM product_performance

  # Failed rows for detailed validation
  - failed rows:
      name: "Invalid pricing rules"
      fail query: |
        SELECT product_id, category, price, cost, profit_margin
        FROM product_performance
        WHERE cost > price 
           OR profit_margin < 0
           OR (category = 'PREMIUM' AND profit_margin < 0.25)

  - failed rows:
      name: "Suspicious sales patterns"  
      fail query: |
        SELECT product_id, sales_count, revenue, price
        FROM product_performance
        WHERE (sales_count > 0 AND revenue = 0)
           OR (sales_count = 0 AND revenue > 0)
           OR (revenue / NULLIF(sales_count, 0) != price)
""",
    "scan_name": "product_performance_business_rules"
}

# Example 4: Time-based data quality monitoring
TIME_BASED_MONITORING = {
    "snowflake_config": {
        "account": "your_account.snowflakecomputing.com",
        "username": "your_username",
        "password": "your_password",
        "database": "EVENT_STREAMING",
        "warehouse": "STREAMING_WH",
        "schema": "EVENTS",
        "role": "STREAM_ANALYST"
    },
    "custom_sql_query": """
                        SELECT event_id,
                               user_id,
                               event_type,
                               event_timestamp,
                               session_id,
                               device_type,
                               country_code
                        FROM user_events
                        WHERE event_timestamp >= DATEADD(hour, -2, CURRENT_TIMESTAMP)
                        """,
    "validation_rules": """
  # Freshness and volume checks
  - row_count > 1000
  - freshness(event_timestamp) < 1h

  # Data completeness
  - missing_count(user_id) = 0
  - missing_count(event_type) = 0
  - missing_percent(device_type) < 5%

  # Event distribution validation
  - event_type_distribution:
      event_type_distribution query: |
        SELECT 
          COUNT(DISTINCT event_type) as unique_event_types
        FROM user_events
        WHERE event_timestamp >= DATEADD(hour, -2, CURRENT_TIMESTAMP)
      warn: when < 5
      fail: when < 3

  # Session quality validation
  - avg_session_events >= 3:
      avg_session_events query: |
        SELECT AVG(event_count) 
        FROM (
          SELECT session_id, COUNT(*) as event_count
          FROM user_events 
          WHERE event_timestamp >= DATEADD(hour, -2, CURRENT_TIMESTAMP)
          GROUP BY session_id
        ) session_stats
""",
    "scan_name": "real_time_event_quality"
}

# Example 5: Financial data validation with strict rules
FINANCIAL_DATA_VALIDATION = {
    "snowflake_config": {
        "account": "your_account.snowflakecomputing.com",
        "username": "your_username",
        "password": "your_password",
        "database": "FINANCIAL_DW",
        "warehouse": "FINANCE_WH",
        "schema": "TRANSACTIONS",
        "role": "FINANCE_ANALYST"
    },
    "custom_sql_query": """
                        SELECT transaction_id,
                               account_id,
                               transaction_date,
                               transaction_type,
                               amount,
                               currency,
                               status,
                               processed_date
                        FROM daily_transactions
                        WHERE transaction_date = CURRENT_DATE - 1
                        """,
    "validation_rules": """
  # Critical data integrity checks
  - row_count > 0:
      name: "Transactions must exist for previous day"

  - missing_count(transaction_id) = 0:
      name: "All transactions must have IDs"

  - missing_count(amount) = 0:
      name: "All transactions must have amounts"

  - duplicate_count(transaction_id) = 0:
      name: "Transaction IDs must be unique"

  # Financial business rules
  - zero_amount_transactions = 0:
      zero_amount_transactions query: |
        SELECT COUNT(*) FROM daily_transactions 
        WHERE amount = 0 AND transaction_type != 'ADJUSTMENT'

  - negative_amounts_validation:
      negative_amounts_validation query: |
        SELECT COUNT(*) FROM daily_transactions 
        WHERE amount < 0 AND transaction_type NOT IN ('REFUND', 'CHARGEBACK', 'REVERSAL')
      fail: when > 0
      name: "Negative amounts only allowed for refunds/chargebacks"

  # Processing time validation
  - processing_delay_hours < 24:
      processing_delay_hours query: |
        SELECT AVG(EXTRACT(hours FROM processed_date - transaction_date))
        FROM daily_transactions 
        WHERE processed_date IS NOT NULL

  # Failed transactions analysis
  - failed rows:
      name: "Invalid transaction data"
      fail query: |
        SELECT transaction_id, account_id, amount, currency, status
        FROM daily_transactions
        WHERE amount IS NULL 
           OR account_id IS NULL
           OR currency IS NULL
           OR currency NOT IN ('USD', 'EUR', 'GBP', 'CAD')
           OR (status = 'COMPLETED' AND processed_date IS NULL)
""",
    "scan_name": "daily_financial_validation"
}


# Example 6: Multi-environment configuration factory
def create_environment_config(environment: str, table_or_query: str, validation_type: str = "basic"):
    """
    Factory function to create environment-specific configurations

    Args:
        environment: 'dev', 'staging', or 'prod'
        table_or_query: Table name or SQL query
        validation_type: 'basic', 'advanced', or 'custom'
    """

    # Environment-specific Snowflake configs
    env_configs = {
        "dev": {
            "account": "dev_account.snowflakecomputing.com",
            "database": "DEV_DB",
            "warehouse": "DEV_WH",
            "schema": "DEV_SCHEMA",
            "role": "DEV_ROLE"
        },
        "staging": {
            "account": "staging_account.snowflakecomputing.com",
            "database": "STAGING_DB",
            "warehouse": "STAGING_WH",
            "schema": "STAGING_SCHEMA",
            "role": "STAGING_ROLE"
        },
        "prod": {
            "account": "prod_account.snowflakecomputing.com",
            "database": "PROD_DB",
            "warehouse": "PROD_WH",
            "schema": "PROD_SCHEMA",
            "role": "PROD_ROLE"
        }
    }

    # Validation rule templates
    validation_templates = {
        "basic": """
  - row_count > 0
  - missing_count(*) = 0
  - duplicate_count(id) = 0
""",
        "advanced": """
  - row_count between 100 and 1000000
  - missing_percent(*) < 1%
  - duplicate_percent(id) = 0%
  - freshness(created_date) < 1d
""",
        "custom": """
  - row_count > 0
  - business_rule_compliance >= 0.95:
      business_rule_compliance query: |
        SELECT COUNT(CASE WHEN your_condition THEN 1 END) * 1.0 / COUNT(*) 
        FROM your_table
"""
    }

    config = {
        "snowflake_config": {
            **env_configs[environment],
            "username": f"{environment}_user",  # You'll need to set actual credentials
            "password": f"{environment}_password"  # Use env vars in production
        },
        "validation_rules": validation_templates[validation_type],
        "scan_name": f"{environment}_{validation_type}_validation"
    }

    # Add table or query
    if table_or_query.upper().startswith('SELECT'):
        config["custom_sql_query"] = table_or_query
    else:
        config["table_name"] = table_or_query

    return config


# Example usage of the factory function:
# dev_config = create_environment_config("dev", "CUSTOMERS", "basic")
# staging_config = create_environment_config("staging", "SELECT * FROM orders WHERE status = 'PENDING'", "advanced")

# Example 7: Configuration with private key authentication (more secure)
PRIVATE_KEY_AUTH_CONFIG = {
    "snowflake_config": {
        "account": "your_account.snowflakecomputing.com",
        "username": "your_service_account",
        # Note: In production, load private key from secure storage
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_path": "/path/to/your/private_key.pem",
        "private_key_passphrase": "your_passphrase",  # Use env var
        "database": "PROD_DB",
        "warehouse": "PROD_WH",
        "schema": "PROD_SCHEMA",
        "role": "SERVICE_ROLE"
    },
    "table_name": "CRITICAL_DATA_TABLE",
    "validation_rules": """
  - row_count > 1000
  - missing_count(critical_field) = 0
  - duplicate_count(unique_id) = 0
""",
    "scan_name": "production_critical_validation"
}


# Example client usage function
async def run_validation_example():
    """
    Example function showing how to call the validation API
    """
    import httpx
    import asyncio

    # Choose configuration
    config = BASIC_TABLE_VALIDATION

    # Make API call
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                "http://localhost:8000/validate",
                json=config,
                timeout=300  # 5 minute timeout
            )

            if response.status_code == 200:
                result = response.json()
                print(f"Validation completed: {result['status']}")
                print(f"Data Quality Score: {result['data_quality_score']:.2%}")
                print(f"Passed: {result['passed_checks']}, Failed: {result['failed_checks']}")

                if result['failed_checks'] > 0:
                    print("\nFailed Checks:")
                    for check in result['check_results']:
                        if check['outcome'] == 'fail':
                            print(f"  - {check['name']}: {check['message'] or 'Failed'}")

            else:
                print(f"Validation failed: {response.status_code}")
                print(response.json())

        except httpx.TimeoutException:
            print("Validation request timed out")
        except Exception as e:
            print(f"Error calling validation API: {e}")


if __name__ == "__main__":
    # Run example (requires running server)
    import asyncio

    asyncio.run(run_validation_example())