from usigrabber.utils.uuid import uuid7


def test_uuid7():
    uuid = uuid7()
    assert len(str(uuid)) == 36
