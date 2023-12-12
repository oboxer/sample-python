from typing import List, Optional

from pydantic import BaseModel, root_validator


class Record(BaseModel):
    Key: Optional[str]
    Data: Optional[str]
    err_msg: Optional[str]

    @root_validator
    def convert_to_lowercase(cls, values):
        raw_key = values.get("Key", "")
        raw_data = values.get("Data", "")

        if raw_key and raw_data:
            values["Key"] = raw_key.lower().strip()
            values["Data"] = raw_data.lower().strip()
        else:
            if not raw_key and not raw_data:
                values["err_msg"] = "Record is empty"
            elif not raw_key:
                values["err_msg"] = "Field `Key` is required"
            elif not raw_data:
                values["err_msg"] = "Field `Data` is required"
        return values

    @staticmethod
    def get_unique_keys() -> List[str]:
        return ["Key", "Data"]

    def is_valid(self) -> bool:
        return True if self.Key and self.Data else False

    def to_err_msg(self) -> Optional[str]:
        return None if self.is_valid() else self.err_msg
