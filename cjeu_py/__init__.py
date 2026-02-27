"""cjeu-py: A Python toolkit for empirical research on the CJEU."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("cjeu-py")
except PackageNotFoundError:
    __version__ = "0.0.0"
