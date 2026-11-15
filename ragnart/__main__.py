"""
Entry point for `python -m ragnart <command>`.

It delegates to ragnart.cli.main, so the module form and the `ragnart` console
script run the same code and exit with the same codes.
"""

import sys

from ragnart.cli import main

if __name__ == "__main__":
    sys.exit(main())
