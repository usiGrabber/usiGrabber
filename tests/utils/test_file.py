from pathlib import Path

from usigrabber.utils.checksum import md5_checksum


def test_md5_checksum(full_mzid_path: Path):
    expected = "ee9e6cf94f58dcda5af2327a2f625346"
    actual = md5_checksum(full_mzid_path)
    assert actual == expected
