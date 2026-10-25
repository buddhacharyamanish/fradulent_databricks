"""
========================================================================================
CDC Pipeline: Schema Manager & Type Inference Test Suite
========================================================================================
Suite Type:
  Offline Unit Testing and Mocked Schema Evolution Testing

Purpose & Objective:
  Validates the offline business logic, configuration parsing, data type inference,
  Debezium envelope unpacking, target table provisioning DDL, and automatic schema
  drift evolution of the CDC pipeline without requiring an active Spark cluster,
  JVM runtime, or live Kafka brokers.

Why Offline Testing:
  - Executes in milliseconds on any developer workstation or lightweight CI/CD runner.
  - Verifies architectural invariants and Delta Lake DDL generation before deploying
    bundles to cloud environments.
  - Ensures schema drift logic handles upstream MySQL modifications cleanly.

Dynamic Architecture:
  - Introspection & Reflection: Test titles, categories, objectives, and checks are
    dynamically extracted from test method docstrings and runtime objects.
  - Dynamic Config Verification: Assertions dynamically compare against the live
    'config/pipeline_metadata.json' file on disk rather than hardcoded static values.
  - Dynamic DDL Validation: Loops over input columns and active governance flags to
    verify generated SQL without static column hardcoding.
========================================================================================
"""

import os
import sys
import json
import time
import unittest
from unittest.mock import MagicMock
from typing import Dict, Any, List

# Ensure project root is available in sys.path when running directly via python3 tests/...
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.cdc.config import PipelineConfig, load_config
from src.cdc.schema_manager import SchemaManager
from src.cdc.preflight import PreflightChecker, PreflightException


class TestConfigAndSchemaManager(unittest.TestCase):
    """
    Test fixture validating configuration loading, type mapping, target table
    provisioning, and schema drift column synchronization.
    """

    def test_01_load_default_config(self):
        """
        Configuration Ingestion & Metadata Discovery
        Test Type: Unit Test (Dynamic Config Loader)
        Objective: Dynamically verifies that load_config() correctly discovers and parses config/pipeline_metadata.json.
        Checks Done: Compares parsed dataclass fields dynamically against JSON file on disk (catalog, schema, table, broker, topic, trigger).
        """
        # Dynamically read the source of truth JSON file on disk
        config_path = os.path.join(project_root, "config", "pipeline_metadata.json")
        with open(config_path, "r", encoding="utf-8") as f:
            raw_json = json.load(f)

        config = load_config()

        # Dynamic assertions matching whatever values are configured in pipeline_metadata.json
        self.assertEqual(config.target.catalog, raw_json["target"]["catalog"])
        self.assertEqual(config.target.schema, raw_json["target"]["schema"])
        self.assertEqual(config.target.table, raw_json["target"]["table"])
        self.assertEqual(
            config.full_table_name,
            f"{raw_json['target']['catalog']}.{raw_json['target']['schema']}.{raw_json['target']['table']}"
        )
        self.assertEqual(config.source.bootstrap_servers, raw_json["source"]["bootstrap_servers"])
        self.assertEqual(config.source.topic, raw_json["source"]["topic"])
        self.assertEqual(config.target.trigger_mode, raw_json["target"]["trigger_mode"])

    def test_02_infer_spark_type(self):
        """
        Python to Spark SQL Type Inference Engine
        Test Type: Unit Test (Data Type Mapping & Schema Integrity)
        Objective: Validates Python primitive to Spark SQL type mapping rules.
        Checks Done: int->BIGINT, float->DOUBLE, bool->BOOLEAN, str->STRING, dict->STRING, list->STRING, None->STRING.
        """
        type_test_matrix = [
            (100, "BIGINT"),
            (99.99, "DOUBLE"),
            (True, "BOOLEAN"),
            ("sample_string", "STRING"),
            ({"nested_key": "val"}, "STRING"),
            (["item_1", "item_2"], "STRING"),
            (None, "STRING")
        ]

        for sample_val, expected_type in type_test_matrix:
            inferred = SchemaManager.infer_spark_type(sample_val)
            self.assertEqual(
                inferred,
                expected_type,
                f"Failed mapping Python value {sample_val} ({type(sample_val)}) to {expected_type}"
            )

    def test_03_column_diffing_and_alter_table(self):
        """
        Schema Drift Detection & Dynamic Column Synchronization
        Test Type: Mocked Integration Test (Schema Evolution & DDL Generation)
        Objective: Simulates upstream schema drift where MySQL table added new columns and verifies ALTER TABLE ADD COLUMN generation.
        Checks Done: Compares existing columns vs incoming fields, isolates new columns, and verifies ALTER TABLE statements.
        """
        mock_spark = MagicMock()
        config = PipelineConfig()
        sm = SchemaManager(spark=mock_spark, config=config)

        # Mock an existing Delta table with an initial subset of columns
        existing_cols = {"id": "bigint", "event_type": "string"}
        sm.get_existing_columns = MagicMock(return_value=existing_cols)

        # Incoming Kafka fields containing existing columns plus new drifted columns
        new_drifted_columns = {
            "user_id": "BIGINT",
            "amount": "DOUBLE",
            "is_flagged": "BOOLEAN"
        }
        incoming_fields = {**{k: v.upper() for k, v in existing_cols.items()}, **new_drifted_columns}

        # Run schema synchronization
        added = sm.sync_table_columns(incoming_fields)

        # Dynamic verification of added columns
        self.assertEqual(set(added), set(new_drifted_columns.keys()))
        for existing_col in existing_cols:
            self.assertNotIn(existing_col, added)

        # Verify exact ALTER TABLE SQL statements generated dynamically
        executed_sqls = [call[0][0] for call in mock_spark.sql.call_args_list]
        for col_name, spark_type in new_drifted_columns.items():
            expected_clause = f"ADD COLUMN `{col_name}` {spark_type}"
            self.assertTrue(
                any(expected_clause in s for s in executed_sqls),
                f"Missing expected DDL clause: '{expected_clause}' in executed SQLs: {executed_sqls}"
            )

    def test_04_create_target_table_ddl(self):
        """
        Target Delta Lake Table Provisioning DDL
        Test Type: Mocked Integration Test (DDL Generation & Governance Invariants)
        Objective: Verifies CREATE TABLE DDL construction dynamically includes all incoming fields and governance metadata.
        Checks Done: Dynamically loops through incoming columns and governance settings to assert SQL presence.
        """
        mock_spark = MagicMock()
        config = PipelineConfig()
        sm = SchemaManager(spark=mock_spark, config=config)

        incoming_fields = {
            "id": "BIGINT",
            "event_id": "STRING",
            "event_type": "STRING"
        }

        sm.create_target_table(incoming_fields)

        self.assertTrue(mock_spark.sql.called)
        executed_sql = mock_spark.sql.call_args[0][0]

        # Dynamic table identification assertions
        self.assertIn("CREATE TABLE IF NOT EXISTS", executed_sql)
        self.assertIn(f"`{sm.catalog}`.`{sm.schema}`.`{sm.table}`", executed_sql)
        self.assertIn("USING DELTA", executed_sql)

        # Dynamic verification that every incoming business field was included
        for col, col_type in incoming_fields.items():
            self.assertIn(f"`{col}` {col_type}", executed_sql)

        # Dynamic verification of governance columns based on active config settings
        if config.metadata_governance.include_raw_payload:
            self.assertIn("`raw_payload` STRING", executed_sql)
        if config.metadata_governance.include_cdc_metadata:
            self.assertIn("`_cdc_op` STRING", executed_sql)
            self.assertIn("`_cdc_ts_ms` BIGINT", executed_sql)
        if config.metadata_governance.include_kafka_metadata:
            self.assertIn("`_kafka_offset` BIGINT", executed_sql)
            self.assertIn("`_ingested_at` TIMESTAMP", executed_sql)

    def test_05_extract_incoming_fields_debezium_envelope(self):
        """
        Debezium CDC Envelope & Field Extraction
        Test Type: Unit Test (Kafka CDC Message Ingestion & Schema Discovery)
        Objective: Verifies extraction of typed business attributes from Debezium JSON Schema descriptor.
        Checks Done: Parses mock Debezium descriptor and validates all dynamic fields mapped to correct Spark types.
        """
        mock_spark = MagicMock()
        config = PipelineConfig()
        sm = SchemaManager(spark=mock_spark, config=config)

        mock_df = MagicMock()
        expected_fields = {
            "id": ("int64", "BIGINT", 1001),
            "event_id": ("string", "STRING", "EVT-TEST-001"),
            "amount": ("double", "DOUBLE", 250.75),
            "is_suspicious": ("boolean", "BOOLEAN", True)
        }

        sample_debezium_msg = {
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "field": "after",
                        "type": "struct",
                        "fields": [
                            {"field": name, "type": meta[0]} for name, meta in expected_fields.items()
                        ]
                    }
                ]
            },
            "payload": {
                "after": {
                    name: meta[2] for name, meta in expected_fields.items()
                }
            }
        }

        mock_df.selectExpr.return_value.limit.return_value.collect.return_value = [
            {"val_str": json.dumps(sample_debezium_msg)}
        ]

        extracted = sm.extract_incoming_fields(mock_df)

        # Dynamic verification against expected fields dictionary
        for field_name, meta in expected_fields.items():
            expected_spark_type = meta[1]
            self.assertEqual(
                extracted.get(field_name),
                expected_spark_type,
                f"Field '{field_name}' expected type '{expected_spark_type}', but got '{extracted.get(field_name)}'"
            )

    def test_06_preflight_config_validation(self):
        """
        Pre-Flight Configuration Invariant Verification
        Test Type: Unit Test (Environment Readiness)
        Objective: Validates that PreflightChecker detects missing configurations and validates complete settings.
        Checks Done: Tests valid configuration passes, and missing bootstrap_servers/catalog/table correctly trigger errors.
        """
        config = load_config()
        mock_spark = MagicMock()
        checker = PreflightChecker(spark=mock_spark, config=config)

        # Valid config passes
        ok, errors = checker.check_configuration()
        self.assertTrue(ok)
        self.assertEqual(len(errors), 0)

        # Corrupted config fails
        corrupted = PipelineConfig()
        corrupted.source.bootstrap_servers = ""
        corrupted.target.catalog = ""
        corrupted_checker = PreflightChecker(spark=mock_spark, config=corrupted)
        ok_corrupted, errors_corrupted = corrupted_checker.check_configuration()
        self.assertFalse(ok_corrupted)
        self.assertTrue(any("Kafka bootstrap servers" in e for e in errors_corrupted))
        self.assertTrue(any("Catalog" in e for e in errors_corrupted))

    def test_07_preflight_kafka_refusal_detection(self):
        """
        Pre-Flight Kafka Failure Mode & Diagnostic Isolation
        Test Type: Mocked Unit Test (Failure Mode Isolation)
        Objective: Validates that PreflightChecker captures ConnectionRefusedError and formats friendly actionable diagnosis.
        Checks Done: Mocks socket.connect raising ConnectionRefusedError, verifies REFUSED status, and checks PreflightException.
        """
        config = load_config()
        mock_spark = MagicMock()
        checker = PreflightChecker(spark=mock_spark, config=config)

        from unittest.mock import patch
        with patch("socket.socket") as mock_socket_cls:
            mock_sock = MagicMock()
            mock_sock.connect.side_effect = ConnectionRefusedError(61, "Connection refused")
            mock_socket_cls.return_value = mock_sock

            all_ok, results = checker.check_kafka_connectivity()
            self.assertFalse(all_ok)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["status"], "REFUSED")
            self.assertIn("stopped or crashed", results[0]["error"])

            # Verify fail-fast exception is raised with clear message
            with self.assertRaises(PreflightException) as ctx:
                checker.run_preflight_suite()
            self.assertIn("unreachable", str(ctx.exception))


def parse_docstring_metadata(doc: str) -> Dict[str, str]:
    """
    Dynamically extracts metadata (Title, Test Type, Objective, Checks)
    from a test method's docstring via reflection.

    Args:
        doc (str): Raw docstring from the test method.

    Returns:
        Dict[str, str]: Parsed metadata dictionary.
    """
    lines = [line.strip() for line in (doc or "").strip().splitlines() if line.strip()]
    metadata = {
        "title": lines[0] if lines else "Untitled Test",
        "type": "Unit Test",
        "objective": "Validation check",
        "checks": "Assertion check"
    }
    for line in lines:
        if line.lower().startswith("test type:"):
            metadata["type"] = line.split(":", 1)[1].strip()
        elif line.lower().startswith("objective:"):
            metadata["objective"] = line.split(":", 1)[1].strip()
        elif line.lower().startswith("checks done:") or line.lower().startswith("checks:"):
            metadata["checks"] = line.split(":", 1)[1].strip()
    return metadata


def run_tests_with_detailed_report() -> bool:
    """
    Executes the test suite with dynamic console reporting.
    All test counts, titles, objectives, and checks are discovered at runtime
    via Python introspection rather than static dictionaries.

    Returns:
        bool: True if all tests passed, False if any failed.
    """
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestConfigAndSchemaManager)
    test_cases = list(suite)
    total_tests = len(test_cases)

    print("=" * 88)
    print("CDC PIPELINE: OFFLINE UNIT & SCHEMA EVOLUTION TEST SUITE")
    print("=" * 88)
    print("Suite Type   : Offline Unit Testing & Mocked Schema Evolution Testing")
    print("Environment  : Local Python 3 (Decoupled from Live JVM, Spark Cluster, and Kafka)")
    print(f"Total Tests  : {total_tests} test cases discovered via reflection")
    print("=" * 88)

    passed_count = 0
    failed_count = 0
    start_total_time = time.time()
    results_summary: List[Dict[str, Any]] = []

    for idx, test_case in enumerate(test_cases, start=1):
        method_name = test_case._testMethodName
        method_obj = getattr(test_case, method_name, None)
        doc = method_obj.__doc__ if method_obj else ""
        meta = parse_docstring_metadata(doc)

        print(f"\n{'-' * 88}")
        print(f"[TEST {idx}/{total_tests}] {meta['title']}")
        print(f"Function     : {method_name}")
        print(f"Test Type    : {meta['type']}")
        print(f"Objective    : {meta['objective']}")
        print(f"Checks Done  : {meta['checks']}")

        test_result = unittest.TestResult()
        t_start = time.time()
        test_case.run(test_result)
        elapsed = time.time() - t_start

        if test_result.wasSuccessful():
            status = "SUCCESS"
            passed_count += 1
            print(f"Status       : [SUCCESS] (Passed in {elapsed:.4f}s)")
        else:
            status = "FAILED"
            failed_count += 1
            print(f"Status       : [FAILED] (Failed in {elapsed:.4f}s)")
            for failure in test_result.failures:
                print(f"Failure Info :\n{failure[1]}")
            for error in test_result.errors:
                print(f"Error Info   :\n{error[1]}")

        results_summary.append({
            "index": idx,
            "name": method_name,
            "title": meta["title"],
            "status": status,
            "time": elapsed
        })

    total_duration = time.time() - start_total_time

    print(f"\n{'=' * 88}")
    print("TEST EXECUTION SUMMARY REPORT")
    print("=" * 88)
    for res in results_summary:
        print(f"  Test {res['index']}: {res['name']:<45} [{res['status']}] ({res['time']:.4f}s)")

    print(f"\nTotal Tests Executed : {len(results_summary)}")
    print(f"Total Passed         : {passed_count}")
    print(f"Total Failed         : {failed_count}")
    print(f"Execution Duration   : {total_duration:.4f}s")
    if failed_count == 0:
        print("Overall Suite Status : SUCCESS (ALL TESTS PASSED)")
    else:
        print("Overall Suite Status : FAILED (FAILURES DETECTED)")
    print("=" * 88)

    return failed_count == 0


if __name__ == "__main__":
    success = run_tests_with_detailed_report()
    sys.exit(0 if success else 1)
