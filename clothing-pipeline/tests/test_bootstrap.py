import unittest
from infra.bootstrap import template
from infra.template import template as pipeline


class BootstrapTests(unittest.TestCase):
    def test_trust_is_exact_repository_branch_and_audience(self):
        role = template("123456789012")["Resources"]["DeployRole"]
        trust = role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]
        self.assertEqual({"token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
            "token.actions.githubusercontent.com:sub": "repo:Sater17@79832416/thread-clothing-data-pipeline@1416576645:ref:refs/heads/main"}, trust["Condition"]["StringEquals"])

    def test_runtime_cannot_manage_iam_and_all_roles_have_boundary(self):
        spec = template("123456789012")
        statements = spec["Resources"]["RuntimeBoundary"]["Properties"]["PolicyDocument"]["Statement"]
        self.assertFalse(any(action.startswith("iam:") for item in statements for action in item["Action"]))
        for item in pipeline()["Resources"].values():
            if item["Type"] == "AWS::IAM::Role":
                self.assertIn("PermissionsBoundary", item["Properties"])
        deploy = spec["Resources"]["DeployRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        create = next(item for item in deploy if "iam:CreateRole" in item["Action"])
        self.assertEqual("arn:aws:iam::123456789012:policy/thread-runtime-boundary", create["Condition"]["StringEquals"]["iam:PermissionsBoundary"])
        self.assertFalse(any(action in ("iam:DeleteRolePermissionsBoundary", "iam:CreatePolicyVersion") for item in deploy for action in item["Action"]))

    def test_existing_provider_is_reused(self):
        self.assertNotIn("GitHubOidc", template("123456789012", provider_exists=True)["Resources"])
