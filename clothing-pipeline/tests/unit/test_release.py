import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from scripts.deploy_release import deploy, verify_release


class ReleaseTests(unittest.TestCase):
    def assets(self, directory):
        checksums = {}
        for name in ("glue_job.py", "thread_runtime.zip", "control.zip", "cloudformation.json"):
            body = b"{}"
            (directory / name).write_bytes(body)
            checksums[name] = hashlib.sha256(body).hexdigest()
        (directory / "release.json").write_text(json.dumps(checksums))

    def test_tampered_release_stops_before_aws_calls(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            self.assets(directory)
            (directory / "control.zip").write_bytes(b"changed")
            session = MagicMock()
            with self.assertRaisesRegex(ValueError, "checksum"):
                deploy(session, directory, "thread-test", "bucket", "thread-runtime/rev1", "apply")
            session.client.assert_not_called()

    def test_plan_does_not_execute_and_apply_waits(self):
        for mode in ("plan-only", "apply"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                self.assets(directory)
                session, s3, cfn = MagicMock(), MagicMock(), MagicMock()
                session.client.side_effect = lambda name: {"s3": s3, "cloudformation": cfn}[name]
                cfn.create_change_set.return_value = {"Id": "change-id"}
                cfn.describe_stacks.return_value = {"Stacks": [{"StackStatus": "CREATE_COMPLETE"}]}
                cfn.describe_change_set.return_value = {"Status": "CREATE_COMPLETE"}
                deploy(session, directory, "thread-test", "bucket", "thread-runtime/rev1", mode)
                parameters = cfn.create_change_set.call_args.kwargs["Parameters"]
                self.assertIn({"ParameterKey": "ScheduleState", "ParameterValue": "DISABLED"}, parameters)
                self.assertEqual(4, s3.put_object.call_count)
                self.assertTrue((directory / "change-set.json").exists())
                if mode == "plan-only":
                    cfn.execute_change_set.assert_not_called()
                else:
                    cfn.execute_change_set.assert_called_once_with(ChangeSetName="change-id")
                    self.assertIn(unittest.mock.call("stack_update_complete"), cfn.get_waiter.call_args_list)

    def test_invalid_mode_has_no_side_effect(self):
        session = MagicMock()
        with self.assertRaisesRegex(ValueError, "mode"):
            deploy(session, ".", "thread-test", "bucket", "thread-runtime/rev1", "auto")
        session.client.assert_not_called()

    def test_unexecuted_create_plan_stays_create_for_apply(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            self.assets(directory)
            session, s3, cfn = MagicMock(), MagicMock(), MagicMock()
            session.client.side_effect = lambda name: {"s3": s3, "cloudformation": cfn}[name]
            cfn.describe_stacks.return_value = {"Stacks": [{"StackStatus": "REVIEW_IN_PROGRESS"}]}
            cfn.create_change_set.return_value = {"Id": "change-id"}
            cfn.describe_change_set.return_value = {"Status": "CREATE_COMPLETE"}
            deploy(session, directory, "thread-test", "bucket", "thread-runtime/rev1", "apply")
            self.assertEqual("CREATE", cfn.create_change_set.call_args.kwargs["ChangeSetType"])
            self.assertIn(unittest.mock.call("stack_create_complete"), cfn.get_waiter.call_args_list)

    def test_update_preserves_email_unless_explicitly_overridden(self):
        for email in (None, "recipient@example.com"):
            with tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                self.assets(directory)
                session, s3, cfn = MagicMock(), MagicMock(), MagicMock()
                session.client.side_effect = lambda name: {"s3": s3, "cloudformation": cfn}[name]
                cfn.describe_stacks.return_value = {"Stacks": [{"StackStatus": "UPDATE_COMPLETE", "Parameters": [
                    {"ParameterKey": "NotificationEmail", "ParameterValue": "previous@example.com"}]}]}
                cfn.create_change_set.return_value = {"Id": "change-id"}
                cfn.describe_change_set.return_value = {"Status": "CREATE_COMPLETE"}
                deploy(session, directory, "thread-test", "bucket", "thread-runtime/rev1", "plan-only", email)
                parameter = next(item for item in cfn.create_change_set.call_args.kwargs["Parameters"] if item["ParameterKey"] == "NotificationEmail")
                assert parameter == ({"ParameterKey": "NotificationEmail", "UsePreviousValue": True} if email is None else
                    {"ParameterKey": "NotificationEmail", "ParameterValue": email})
