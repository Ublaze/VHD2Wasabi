"""Profile portability tests: export/import round-trip plain + encrypted,
named-profile store, legacy migration, validation. No S3 needed."""
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))

from engine import profiles as profiles_mod
from engine.compressor import find_sevenzip

SAMPLE = {
    "endpoint": "s3.eu-central-1.wasabisys.com",
    "bucket": "my-bucket",
    "access_key": "AKIATEST123",
    "secret_key": "supersecret",
    "prefix": "vhd2wasabi",
    "segment_mb": "2048",
    "level": "3",
    "merge": False,
    "delete_segments": True,
}


def main():
    tmp = tempfile.mkdtemp(prefix="vhd2w-profiles-")
    profiles_mod.APP_DIR = os.path.join(tmp, "appdata")
    profiles_mod.PROFILES_PATH = os.path.join(profiles_mod.APP_DIR, "profiles.json")
    try:
        # named store + last_used
        profiles_mod.save_profile("prod", SAMPLE)
        profiles_mod.save_profile("dr", dict(SAMPLE, bucket="other"))
        allp, last = profiles_mod.load_all()
        assert set(allp) == {"prod", "dr"} and last == "dr"
        assert allp["prod"]["bucket"] == "my-bucket"
        print("named store + last_used: OK")

        # plain export -> import round-trip
        p1 = os.path.join(tmp, "prod.vhd2wprofile")
        profiles_mod.export_profile("prod", SAMPLE, p1)
        head = json.load(open(p1, encoding="utf-8"))
        assert head["tool"] == "VHD2Wasabi" and head["format_version"] == 1
        assert head["exported_at"] and head["name"] == "prod"
        name, prof = profiles_mod.import_profile(p1)
        assert name == "prod"
        assert prof == profiles_mod.normalize(SAMPLE), "plain round-trip mismatch"
        print("plain export/import round-trip: OK")

        # encrypted round-trip
        if find_sevenzip():
            p2 = os.path.join(tmp, "prod-enc.7z")
            profiles_mod.export_profile("prod", SAMPLE, p2, password="pw123")
            assert profiles_mod.is_7z(p2)
            with open(p2, "rb") as f:
                assert b"supersecret" not in f.read(), "secret leaked into archive"
            name, prof = profiles_mod.import_profile(p2, password="pw123")
            assert name == "prod" and prof == profiles_mod.normalize(SAMPLE)
            print("encrypted export/import round-trip: OK")
            try:
                profiles_mod.import_profile(p2, password="wrong")
            except profiles_mod.ProfileError:
                print("wrong password correctly rejected: OK")
            else:
                raise AssertionError("wrong password must fail")
            try:
                profiles_mod.import_profile(p2)
            except profiles_mod.ProfileError:
                print("missing password correctly rejected: OK")
            else:
                raise AssertionError("missing password must fail")
        else:
            print("7za missing — encrypted tests skipped")

        # validation
        try:
            profiles_mod.export_profile("bad", dict(SAMPLE, secret_key=""),
                                        os.path.join(tmp, "bad.json"))
        except profiles_mod.ProfileError:
            print("required-field validation: OK")
        else:
            raise AssertionError("missing secret_key must fail validation")

        # legacy flat file migrates to 'default'
        with open(profiles_mod.PROFILES_PATH, "w", encoding="utf-8") as f:
            json.dump(SAMPLE, f)
        allp, last = profiles_mod.load_all()
        assert "default" in allp and allp["default"]["bucket"] == "my-bucket"
        print("legacy profiles.json migration: OK")

        print("test_profiles: ALL PASSED")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
