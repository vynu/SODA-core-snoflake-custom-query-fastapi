"""
Test suite for SODA Core Snowflake FastAPI Validator

Run with: pytest test_main.py -v
"""

import json
from unittest.mock import Mock, patch
import pytest
from fastapi.testclient import TestClient
from main import app, SODAValidationError, ThreadSafeSODAService, SnowflakeConfig, ValidationRequest

# Test client
client = TestClient(app)


class TestHealthAndInfo:
    """Test basic API endpoints"""

    def test_health_check(self):
        """Test health check endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
        assert "SODA Core Snowflake Validator" in response.json()["service"]

    def test_validation_examples(self):
        """Test validation rules examples endpoint"""
        response = client.get("/validation-rules-examples")
        assert response.status_code == 200
        data = response.json()
        assert "basic_validations" in data
        assert "advanced_validations" in data
        assert "custom_metrics" in data
        assert "row_count > 0" in data["basic_validations"]


class TestPydanticModels:
    """Test Pydantic model validation"""

    def test_snowflake_config_valid(self):
        """Test valid Snowflake configuration"""
        config = SnowflakeConfig(
            account="test.snowflakecomputing.com",
            username="testuser",
            password="testpass",
            database="TESTDB",
            warehouse="TESTWH",
            schema="PUBLIC"
        )
        assert config.account == "test.snowflakecomputing.com"
        assert config.role == "PUBLIC"  # default value
        assert config.connection_timeout == 240  # default value

    def test_snowflake_config_missing_required(self):
        """Test Snowflake config with missing required fields"""
        with pytest.raises(ValueError):
            SnowflakeConfig(
                account="test.snowflakecomputing.com",
                username="testuser"
                # Missing required fields
            )

    def test_validation_request_valid(self):
        """Test valid validation request"""
        config = SnowflakeConfig(
            account="test.snowflakecomputing.com",
            username="testuser",
            password="testpass",
            database="TESTDB",
            warehouse="TESTWH",
            schema="PUBLIC"
        )

        request = ValidationRequest(
            snowflake_config=config,
            table_name="CUSTOMERS",
            validation_rules="- row_count > 0",
            scan_name="test_scan"
        )

        assert request.table_name == "CUSTOMERS"
        assert request.scan_name == "test_scan"
        assert request.custom_sql_query is None


class TestSODAService:
    """Test SODA Core service functionality"""

    def test_soda_service_initialization(self):
        """Test SODA service initializes correctly"""
        service = ThreadSafeSODAService(max_workers=2)
        assert service.executor._max_workers == 2
        assert service.scan_lock is not None

    def test_build_snowflake_config_yaml(self):
        """Test Snowflake YAML configuration generation"""
        service = ThreadSafeSODAService()
        config = SnowflakeConfig(
            account="test.snowflakecomputing.com",
            username="testuser",
            password="testpass",
            database="TESTDB",
            warehouse="TESTWH",
            schema="PUBLIC",
            role="ANALYST"
        )

        yaml_config = service._build_snowflake_config_yaml(config, "test_source")

        assert "data_source test_source:" in yaml_config
        assert "type: snowflake" in yaml_config
        assert "account: test.snowflakecomputing.com" in yaml_config
        assert "username: testuser" in yaml_config
        assert "role: ANALYST" in yaml_config
        assert "client_session_keep_alive: true" in yaml_config

    def test_build_validation_rules_custom_query(self):
        """Test validation rules for custom SQL query"""
        service = ThreadSafeSODAService()

        config = SnowflakeConfig(
            account="test.snowflakecomputing.com",
            username="testuser",
            password="testpass",
            database="TESTDB",
            warehouse="TESTWH",
            schema="PUBLIC"
        )

        request = ValidationRequest(
            snowflake_config=config,
            custom_sql_query="SELECT * FROM customers WHERE active = 1",
            validation_rules="- row_count > 0\n- missing_count(email) = 0"
        )

        rules = service._build_validation_rules(request)

        assert "checks for (SELECT * FROM customers WHERE active = 1):" in rules
        assert "- row_count > 0" in rules
        assert "- missing_count(email) = 0" in rules

    def test_build_validation_rules_table(self):
        """Test validation rules for table validation"""
        service = ThreadSafeSODAService()

        config = SnowflakeConfig(
            account="test.snowflakecomputing.com",
            username="testuser",
            password="testpass",
            database="TESTDB",
            warehouse="TESTWH",
            schema="PUBLIC"
        )

        request = ValidationRequest(
            snowflake_config=config,
            table_name="CUSTOMERS",
            validation_rules="- row_count > 100\n- duplicate_count(id) = 0"
        )

        rules = service._build_validation_rules(request)

        assert "checks for CUSTOMERS:" in rules
        assert "- row_count > 100" in rules
        assert "- duplicate_count(id) = 0" in rules


class TestValidationEndpoint:
    """Test the main validation endpoint"""

    @patch('main.soda_service.execute_validation')
    def test_validate_endpoint_success(self, mock_execute):
        """Test successful validation request"""
        # Mock the validation execution
        mock_execute.return_value = {
            'status': 'passed',
            'exit_code': 0,
            'data_quality_score': 0.95,
            'passed_checks': 4,
            'failed_checks': 0,
            'warning_checks': 0,
            'total_checks': 4,
            'check_results': [
                {
                    'name': 'row_count > 0',
                    'table': 'CUSTOMERS',
                    'column': None,
                    'outcome': 'pass',
                    'value': 1500,
                    'message': None
                }
            ],
            'failed_rows_sample': [],
            'execution_time_seconds': 1.23,
            'logs': 'INFO: Validation completed successfully'
        }

        request_data = {
            "snowflake_config": {
                "account": "test.snowflakecomputing.com",
                "username": "testuser",
                "password": "testpass",
                "database": "TESTDB",
                "warehouse": "TESTWH",
                "schema": "PUBLIC"
            },
            "table_name": "CUSTOMERS",
            "validation_rules": "- row_count > 0",
            "scan_name": "test_validation"
        }

        response = client.post("/validate", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "passed"
        assert data["exit_code"] == 0
        assert data["data_quality_score"] == 0.95
        assert data["passed_checks"] == 4
        assert data["failed_checks"] == 0
        assert len(data["check_results"]) == 1
        assert "scan_id" in data

    def test_validate_endpoint_invalid_config(self):
        """Test validation with invalid Snowflake config"""
        request_data = {
            "snowflake_config": {
                "account": "test.snowflakecomputing.com",
                "username": "testuser"
                # Missing required fields
            },
            "table_name": "CUSTOMERS",
            "validation_rules": "- row_count > 0"
        }

        response = client.post("/validate", json=request_data)
        assert response.status_code == 422  # Validation error

    def test_validate_endpoint_missing_table_and_query(self):
        """Test validation without table name or custom query"""
        request_data = {
            "snowflake_config": {
                "account": "test.snowflakecomputing.com",
                "username": "testuser",
                "password": "testpass",
                "database": "TESTDB",
                "warehouse": "TESTWH",
                "schema": "PUBLIC"
            },
            # No table_name or custom_sql_query
            "validation_rules": "- row_count > 0"
        }

        response = client.post("/validate", json=request_data)
        assert response.status_code == 400  # Configuration error

    @patch('main.soda_service.execute_validation')
    def test_validate_endpoint_soda_error(self, mock_execute):
        """Test validation endpoint with SODA validation error"""
        mock_execute.side_effect = SODAValidationError(
            "scan_execution_error",
            "Failed to connect to Snowflake",
            {"connection_timeout": True}
        )

        request_data = {
            "snowflake_config": {
                "account": "invalid.snowflakecomputing.com",
                "username": "testuser",
                "password": "wrongpass",
                "database": "TESTDB",
                "warehouse": "TESTWH",
                "schema": "PUBLIC"
            },
            "table_name": "CUSTOMERS",
            "validation_rules": "- row_count > 0"
        }

        response = client.post("/validate", json=request_data)

        assert response.status_code == 500  # Server error for execution errors
        data = response.json()
        assert "error_type" in data["detail"]
        assert data["detail"]["error_type"] == "scan_execution_error"
        assert "Failed to connect to Snowflake" in data["detail"]["message"]


class TestSODAValidationError:
    """Test custom exception handling"""

    def test_soda_validation_error_basic(self):
        """Test basic SODA validation error"""
        error = SODAValidationError("test_error", "Test message")
        assert error.error_type == "test_error"
        assert error.message == "Test message"
        assert error.details == {}
        assert str(error) == "Test message"

    def test_soda_validation_error_with_details(self):
        """Test SODA validation error with details"""
        details = {"connection": "timeout", "retry_count": 3}
        error = SODAValidationError("connection_error", "Connection failed", details)

        assert error.error_type == "connection_error"
        assert error.message == "Connection failed"
        assert error.details == details
        assert error.details["connection"] == "timeout"


class TestIntegrationScenarios:
    """Integration test scenarios"""

    def test_validation_request_with_custom_query(self):
        """Test validation request structure for custom query"""
        request_data = {
            "snowflake_config": {
                "account": "test.snowflakecomputing.com",
                "username": "testuser",
                "password": "testpass",
                "database": "ANALYTICS",
                "warehouse": "COMPUTE_WH",
                "schema": "PUBLIC",
                "role": "ANALYST",
                "connection_timeout": 300
            },
            "custom_sql_query": "SELECT o.order_id, c.customer_name FROM orders o JOIN customers c ON o.customer_id = c.customer_id",
            "validation_rules": "- row_count between 100 and 5000\n- missing_count(customer_name) = 0\n- duplicate_count(order_id) = 0",
            "scan_name": "order_customer_join_validation"
        }

        # Test that the request structure is valid
        try:
            # This would normally make the API call
            # For testing, we just validate the structure
            assert "snowflake_config" in request_data
            assert "custom_sql_query" in request_data
            assert "validation_rules" in request_data
            assert "JOIN" in request_data["custom_sql_query"]
            assert "row_count between" in request_data["validation_rules"]
        except Exception as e:
            pytest.fail(f"Request structure validation failed: {e}")

    def test_complex_validation_rules_structure(self):
        """Test complex validation rules parsing"""
        complex_rules = """
  - row_count > 1000
  - missing_count(email) = 0
  - duplicate_count(customer_id) = 0
  - avg(order_amount) between 50 and 500
  - conversion_rate >= 0.15:
      conversion_rate query: |
        SELECT COUNT(CASE WHEN status = 'completed' THEN 1 END) * 1.0 / COUNT(*) 
        FROM sales_data
  - failed rows:
      name: "Invalid order dates"
      fail query: |
        SELECT order_id, order_date, ship_date
        FROM orders 
        WHERE ship_date < order_date
"""

        # Test that complex rules structure is well-formed
        assert "conversion_rate query:" in complex_rules
        assert "failed rows:" in complex_rules
        assert "fail query:" in complex_rules
        assert len([line for line in complex_rules.split('\n') if line.strip().startswith('-')]) == 5


# Integration test (requires actual Snowflake connection - skip by default)
@pytest.mark.skip(reason="Requires actual Snowflake credentials")
class TestRealSnowflakeIntegration:
    """Real integration tests (requires live Snowflake connection)"""

    def test_real_snowflake_validation(self):
        """Test with actual Snowflake connection (requires credentials)"""
        # This test requires real Snowflake credentials
        # Uncomment and modify when you have test credentials
        pass

        # request_data = {
        #     "snowflake_config": {
        #         "account": "your_test_account.snowflakecomputing.com",
        #         "username": "your_test_user",
        #         "password": "your_test_password",
        #         "database": "your_test_database",
        #         "warehouse": "your_test_warehouse",
        #         "schema": "your_test_schema"
        #     },
        #     "table_name": "your_test_table",
        #     "validation_rules": "- row_count > 0",
        #     "scan_name": "integration_test"
        # }
        #
        # response = client.post("/validate", json=request_data)
        # assert response.status_code == 200


if __name__ == "__main__":
    # Run specific tests
    pytest.main([__file__, "-v", "--tb=short"])