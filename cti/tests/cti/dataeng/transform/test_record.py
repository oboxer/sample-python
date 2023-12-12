import pytest

from ctizh.dataeng.transform.record import Record


class TestUnitRecord:

    @pytest.mark.parametrize(
        "key, data, expected_key, expected_data, error_message",
        [
            ("EXAMPLE", "Some Data", "example", "some data", ""),
            ("  example  ", "  some data  ", "example", "some data", ""),
            ("", "Some Data", None, None, "Field `Key` is required"),
            ("Example", "", None, None, "Field `Data` is required"),
            ("", "", None, None, "Record is empty"),
            (None, "Some Data", None, None, "Field `Key` is required"),
            ("Example", None, None, None, "Field `Data` is required"),
        ],
    )
    def test_record(
        self,
        key,
        data,
        expected_key,
        expected_data,
        error_message,
    ):

        record = Record(Key=key, Data=data)
        if error_message:
            assert record.is_valid() is False
            assert record.to_err_msg() == error_message
        else:
            assert record.Key == expected_key
            assert record.Data == expected_data
            assert record.is_valid() is True
            assert record.to_err_msg() is None
