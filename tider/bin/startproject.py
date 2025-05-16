import re
import click
from pathlib import Path
from importlib.util import find_spec
from shutil import copy2, copystat, ignore_patterns, move
from stat import S_IWUSR as OWNER_WRITE_PERMISSION

import tider

IGNORE = ignore_patterns("*.pyc", "__pycache__", ".svn", ".git")


def _make_writable(path: Path) -> None:
    current_permissions = path.stat().st_mode
    path.chmod(current_permissions | OWNER_WRITE_PERMISSION)


def _copytree(src: Path, dst: Path) -> None:
    """
    Copied from Scrapy.

    More info at:
    https://github.com/scrapy/scrapy/pull/2005
    """
    ignore = IGNORE
    names = [x.name for x in src.iterdir()]
    ignored_names = ignore(src, names)

    if not dst.exists():
        dst.mkdir(parents=True)

    for name in names:
        if name in ignored_names:
            continue

        srcname = src / name
        dstname = dst / name
        if srcname.is_dir():
            _copytree(srcname, dstname)
        else:
            copy2(srcname, dstname)
            _make_writable(dstname)

    copystat(src, dst)
    _make_writable(dst)


def _is_valid_name(project_name: str) -> bool:
    def _module_exists(module_name: str) -> bool:
        spec = find_spec(module_name)
        return spec is not None and spec.loader is not None

    if not re.search(r"^[_a-zA-Z]\w*$", project_name):
        print(
            "Error: Project names must begin with a letter and contain"
            " only\nletters, numbers and underscores"
        )
    elif _module_exists(project_name):
        print(f"Error: Module {project_name!r} already exists")
    else:
        return True
    return False


@click.command()
@click.option('--project-name',
              type=str,
              help="Project name.")
@click.option('--project-dir',
              type=str,
              default=None,
              help="Project directory.")
@click.option('--templates-dir',
              type=str,
              default=None,
              help="Template directory.")
@click.pass_context
def startproject(ctx, project_name, project_dir=None, templates_dir=None):
    """Start a tider project."""
    if not _is_valid_name(project_name):
        ctx.exit(1)
        return
    if not project_dir:
        project_dir = project_name
    project_dir = Path(project_dir)
    if project_dir.exists():
        click.echo(click.style(f'Project already exists', fg="yellow", bold=True))
        ctx.exit(1)
    templates_dir = Path(templates_dir or Path(tider.__path__[0], "templates"), 'project')
    _copytree(Path(templates_dir), project_dir.resolve())
    move(project_dir / "module", project_dir / project_name)
    click.echo(click.style(f'New Tider project "{project_name}" created in: {project_dir.resolve()}', fg="green", bold=True))
    ctx.exit(0)
