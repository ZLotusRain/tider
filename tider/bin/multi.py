"""Start all spiders in specific schema from the command-line.

.. program:: tider multi

Examples
========

.. code-block:: console

    $ tider multi start Test
"""

import os
import sys
import shlex
import click
import platform
import subprocess


IS_WINDOWS = platform.system() == 'Windows'
TIDER_EXE = 'tider'


def tider_exe(*args):
    return ' '.join((TIDER_EXE,) + args)


class MultiTool:

    def __init__(self, spiders, schema=None, cmd=None):
        self.spiders = spiders
        self.schema = schema
        self.cmd = cmd or f"-m {TIDER_EXE}"
        self.prog_name = 'tider multi'
        self.commands = {
            'start': self.start
        }

    @staticmethod
    def prepare_argv(argv, path):
        args = ' '.join([path] + list(argv))
        return shlex.split(args, posix=not IS_WINDOWS)

    def execute_from_commandline(self, argv, cmd=None):
        self.cmd = cmd if cmd is not None else self.cmd
        self.prog_name = os.path.basename(argv.pop(0))

        if not self.validate_arguments(argv):
            raise click.UsageError(f"Wrong arguments: {argv}")
        return self.call_command(argv[0], argv[1:])

    @staticmethod
    def validate_arguments(argv):
        return argv and argv[0][0] != '-'

    def call_command(self, command, argv):
        try:
            return self.commands[command](*argv)
        except KeyError:
            print(f'Invalid command: {command}', end='\n')

    def start(self, *argv):
        print(f'> Starting schema: {self.schema}...', end='\n')
        for spider in self.spiders:
            crawl_argv = tuple(self.cmd.split(" ")) + argv + ('crawl', '--spider', spider, '-D')
            argstr = self.prepare_argv(crawl_argv, path=sys.executable)
            pipe = subprocess.Popen(argstr, stdout=subprocess.PIPE)
            pipe.wait()


@click.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True
    }
)
@click.pass_context
def multi(ctx):
    crawler_process = ctx.obj
    spiders = crawler_process.spider_loader.list()
    schema = crawler_process.schema
    cmd = MultiTool(spiders=spiders, schema=schema)
    args = sys.argv[1:]
    # rearrange the arguments so that the MultiTool will parse them correctly.
    args = args[args.index('multi'):] + args[:args.index('multi')]
    return cmd.execute_from_commandline(args)
