#! /usr/bin/env python
import sys
import logging

from upload_spatial_measures import run_upload_measures

logging.basicConfig(level=logging.INFO)

SUBCOMMANDS = {
    "upload_measures": run_upload_measures
}

if __name__ == "__main__":
    logging.info("Running %s", sys.argv)

    if len(sys.argv) < 2 or sys.argv[1] not in SUBCOMMANDS.keys():
        logging.info("No subcommand specified. Run pipeline [SUBCOMMAND], where subcommand is one of %s", SUBCOMMANDS.keys())
        exit(1)

    subcommand = sys.argv[1]
    subcommand_args = sys.argv[2:]

    SUBCOMMANDS[subcommand](subcommand_args)
