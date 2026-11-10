"""ne_rollout.audit_manifest: current-version products are clean even without a status flag."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import ne_rollout as nr
from v21_products import MANIFEST_VERSION as CUR


def test_current_version_without_status_is_clean():
    ent = {"10000-sw1_json_v2": {"size": 10, "version": CUR},                 # sweep unit, status missing
           "10000_json_v2": {"size": 10, "version": CUR, "status": "cf"},
           "10001_json_v2": {"size": 10, "version": "v2.2"}}                  # pre-2.4, really suspect
    a = nr.audit_manifest(ent)
    assert a["_suspect_code_set"] == {"10001"}
    assert a["suspect_files"] == 1


if __name__ == "__main__":
    test_current_version_without_status_is_clean(); print("ok")
