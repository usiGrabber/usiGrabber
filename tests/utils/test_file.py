from pathlib import Path

from usigrabber.utils.checksum import md5_checksum


def test_md5_checksum(full_mzid_path: Path):
    expected = "ee9e6cf94f58dcda5af2327a2f625346"
    actual = md5_checksum(full_mzid_path)
    assert actual == expected


def test_md5_checksum_of_multiple_files(full_mzid_path: Path, mzid_file_path_2: Path):
    expected = "729eb4cd9a02624693b92476ae4fca8a"
    actual = md5_checksum((full_mzid_path, mzid_file_path_2))
    actual_different_ordering = md5_checksum((mzid_file_path_2, full_mzid_path))

    assert actual == expected
    assert actual_different_ordering == expected
