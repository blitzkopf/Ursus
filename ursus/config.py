"""Module to read configuration from a file and command line arguments."""

import argparse
import logging
import sys
from configparser import ConfigParser


def init_config(argv=None):
    """Initialize the configuration."""
    # Do argv default this way, as doing it in the functional
    # declaration sets it at compile time.
    if argv is None:
        argv = sys.argv

    global config
    defaults = {"LogLevel": "INFO"}
    # https://stackoverflow.com/questions/3609852/which-is-the-best-way-to-allow-configuration-options-be-overridden-at-the-comman
    pre_argp = argparse.ArgumentParser(
        description=__doc__,  # printed with -h/--help
        # Don't mess with format of description
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # Turn off help, so we print all options in response to -h
        add_help=False,
    )
    # Add an optional string argument 'config'
    pre_argp.add_argument("-c", "--config", dest="config_file", default="ursus.cfg", type=str)
    pre_argp.add_argument(
        "-l",
        "--log",
        help="Set logging level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        nargs="?",
        dest="log_level",
        const="INFO",
        default=defaults["LogLevel"],
        type=str.upper,
    )

    args, remaining_argv = pre_argp.parse_known_args()
    ## FIXME: LogLEvel in config file is ignored
    logging.basicConfig(level=args.log_level)

    if args.config_file:
        config_file = args.config_file
        config = ConfigParser()
        logging.info("Reading configuration from %s" % (config_file))
        config.read([config_file])
        defaults.update(dict(config.items("GENERAL")))

    argp = argparse.ArgumentParser(
        # Inherit options from config_parser
        parents=[pre_argp]
    )
    argp.set_defaults(**defaults)
    ## argp.add_argument("--option")
    args, remaining_argv = argp.parse_known_args(remaining_argv)
    logging.basicConfig(level=args.log_level)
    return config, remaining_argv
