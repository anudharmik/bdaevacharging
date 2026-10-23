"""
Unit tests for verifying Phase 1 project scaffolding, file existence, and configurations.
Compatible with both standard library unittest and pytest.
"""

import unittest
from pathlib import Path


class TestProjectScaffold(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parent.parent

    def test_directory_scaffold_exists(self):
        expected_dirs = [
            "data/raw",
            "data/processed",
            "src",
            "src/analytics",
            "src/database",
            "src/dashboard",
            "cassandra",
            "tests",
            "docs",
            "scripts",
        ]
        for rel_path in expected_dirs:
            d = self.root / rel_path
            self.assertTrue(d.exists(), f"Required directory {rel_path} does not exist.")
            self.assertTrue(d.is_dir(), f"Path {rel_path} is not a directory.")

    def test_required_configuration_files_exist(self):
        expected_files = [
            "docker-compose.yml",
            "requirements.txt",
            ".gitignore",
            ".env.example",
            "README.md",
            "scripts/check_env.py",
            "docs/project_report.md",
            "docs/viva_questions.md",
        ]
        for rel_path in expected_files:
            f = self.root / rel_path
            self.assertTrue(f.exists(), f"Required file {rel_path} does not exist.")
            self.assertTrue(f.is_file(), f"Path {rel_path} is not a regular file.")

    def test_docker_compose_configuration(self):
        dc_file = self.root / "docker-compose.yml"
        content = dc_file.read_text(encoding="utf-8")

        self.assertIn("cassandra:", content)
        self.assertIn("9042:9042", content)
        self.assertIn("healthcheck:", content)
        self.assertIn("ev_cassandra", content)

    def test_gitignore_covers_sensitive_items(self):
        gi_file = self.root / ".gitignore"
        content = gi_file.read_text(encoding="utf-8")

        self.assertIn(".env", content)
        self.assertIn(".venv", content)
        self.assertIn("data/raw/*", content)
        self.assertIn("!data/raw/.gitkeep", content)


if __name__ == "__main__":
    unittest.main()
