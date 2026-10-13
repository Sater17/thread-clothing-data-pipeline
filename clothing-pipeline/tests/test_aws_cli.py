import subprocess
import unittest
from unittest.mock import patch

from scripts.aws_cli import call


class AwsCliTests(unittest.TestCase):
    def test_conditional_upload_uses_exec_and_literal_wildcard(self):
        with patch("scripts.aws_cli.subprocess.run", return_value=subprocess.CompletedProcess([], 0, "{}", "")) as run:
            call(["s3api", "put-object", "--if-none-match", "*", "--key", "thread/input/a b.json"])
        args = run.call_args.args[0]
        self.assertEqual(["wsl.exe", "-d", "Ubuntu", "--exec", "/home/sater/.local/bin/aws"], args[:5])
        self.assertEqual("*", args[args.index("--if-none-match") + 1])
        self.assertEqual("thread/input/a b.json", args[args.index("--key") + 1])
        self.assertNotIn("shell", run.call_args.kwargs)
