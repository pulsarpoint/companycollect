import unittest

from click.testing import CliRunner

from ex4.main import cli


class LabCliTest(unittest.TestCase):
    def test_all_subcommands_expose_help(self) -> None:
        runner = CliRunner()
        for args in (
            ["sites", "verify"],
            ["candidates", "build"],
            ["gold", "draft"],
            ["run"],
            ["score"],
            ["report"],
        ):
            result = runner.invoke(cli, [*args, "--help"])
            self.assertEqual(result.exit_code, 0, args)
        run_help = runner.invoke(cli, ["run", "--help"]).output
        self.assertIn("--dry-run", run_help)
        self.assertIn("default: 2", run_help)
