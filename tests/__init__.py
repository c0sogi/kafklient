"""Test suite package.

Several tests import helpers via `tests._config`, so this directory must be importable.
"""

"""Test package for local development.

This file exists so the `kafklient-test` entrypoint (`tests.__main__:main`) can import
the `tests` module when running from the repository root.
"""
