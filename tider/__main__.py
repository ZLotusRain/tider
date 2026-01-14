"""Entry-point for the :program:`tider` umbrella command."""

import sys
from . import maybe_patch_concurrency, maybe_patch_third_party


__all__ = ('main',)


def maybe_patch():
    if 'multi' in sys.argv:
        return
    if '-D' in sys.argv:
        return
    if '--detach' in sys.argv:
        return
    maybe_patch_concurrency()
    maybe_patch_third_party()


def main():
    """Entrypoint to the ``tider`` umbrella command."""
    maybe_patch()
    from tider.bin.tider import main as _main
    sys.exit(_main())


if __name__ == "__main__":   # pragma: no cover
    main()
