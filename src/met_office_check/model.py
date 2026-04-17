from enum import StrEnum


class Model(StrEnum):
    GLOBAL = "global"
    UK = "uk"

    @property
    def s3_prefix(self) -> str:
        match self:
            case Model.GLOBAL:
                return "global-deterministic-10km"
            case Model.UK:
                return "uk-deterministic-2km"
