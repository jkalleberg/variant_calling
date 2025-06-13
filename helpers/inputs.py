from argparse import Namespace
from logging import Logger
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Inputs:
    """
    Save the user-provided inputs for repeated use.
    """
    # required parameters
    args: Namespace
    logger: Logger
    phase: str

    def __post_init__(self) -> None:
        if "metadata" in self.args and self.args.metadata is not None:
            self.metadata = Path(self.args.metadata)
        if "output_dir" in self.args:
            self.output_dir = Path(self.args.output_dir)

    def update_mode(self) -> None:
        self.overwrite = self.args.overwrite
        self.debug_mode = self.args.debug
        self.dry_run_mode = self.args.dry_run

    def create_logging_msg(self) -> None:
        if self.args.dry_run:
            self.logger_msg = f"[DRY_RUN] - [{self.phase}]"
        else:
            self.logger_msg = f"[{self.phase}]"

    def create_a_dir(self, dir_name: Path) -> None:
        """
        Create a new directory.
        """
        if not dir_name.exists() or not dir_name.is_dir():
            if self.dry_run_mode:
                self.logger.info(
                    f"{self.logger_msg}: pretending to create a new directory | '{str(dir_name)}'"
                )
            else:
                if self.debug_mode:
                    self.logger.debug(
                        f"{self.logger_msg}: creating a new directory | '{str(dir_name)}'"
                    )
                dir_name.mkdir(exist_ok=True, parents=True)
