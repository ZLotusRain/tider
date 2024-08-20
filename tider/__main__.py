"""Entry-point for the :program:`tider` umbrella command."""

import sys
from . import maybe_patch_concurrency


__all__ = ('main',)


def main():
    """Entrypoint to the ``tider`` umbrella command."""
    if 'multi' not in sys.argv:
        maybe_patch_concurrency()
    from tider.bin.tider import main as _main
    sys.exit(_main())


if __name__ == "__main__":   # pragma: no cover
    main()
