"""Start multiple spiders from the command-line.

.. program:: tider multi

Examples
========

.. code-block:: console

    $ tider multi start Spider 3 --settings=settings --schema=schema
"""
import os
import sys
import shlex
import click
import subprocess
from functools import wraps
from kombu.utils.encoding import from_utf8
from kombu.utils.objects import cached_property
from collections import OrderedDict, UserList, defaultdict

from tider.platforms import EX_OK, EX_FAILURE, IS_WINDOWS

TIDER_EXE = 'tider'


def tider_exe(*args):
    return ' '.join((TIDER_EXE,) + args)


def maybe_call(fun, *args, **kwargs):
    if fun is not None:
        fun(*args, **kwargs)


def format_opt(opt, value):
    if not value:
        return opt
    if opt.startswith('--'):
        return f'{opt}={value}'
    return f'{opt} {value}'


def using_cluster(fun):

    @wraps(fun)
    def _inner(self, *argv, **kwargs):
        return fun(self, self.cluster_from_argv(argv), **kwargs)
    return _inner


class OptionParser:

    def __init__(self, args):
        self.args = args
        self.options = OrderedDict()
        self.values = []
        self.passthrough = ''
        self.namespaces = defaultdict(lambda: OrderedDict())

    def parse(self):
        rargs = [arg for arg in self.args if arg]
        pos = 0
        while pos < len(rargs):
            arg = rargs[pos]
            if arg == '--':
                self.passthrough = ' '.join(rargs[pos:])
                break
            elif arg[0] == '-':
                if arg[1] == '-':
                    self.process_long_opt(arg[2:])
                else:
                    value = None
                    if len(rargs) > pos + 1 and rargs[pos + 1][0] != '-':
                        value = rargs[pos + 1]
                        pos += 1
                    self.process_short_opt(arg[1:], value)
            else:
                self.values.append(arg)
            pos += 1

    def process_long_opt(self, arg, value=None):
        if '=' in arg:
            arg, value = arg.split('=', 1)
        self.add_option(arg, value, short=False)

    def process_short_opt(self, arg, value=None):
        self.add_option(arg, value, short=True)

    def optmerge(self, ns, defaults=None):
        if defaults is None:
            defaults = self.options
        return OrderedDict(defaults, **self.namespaces[ns])

    def add_option(self, name, value, short=False, ns=None):
        prefix = short and '-' or '--'
        dest = self.options
        if ':' in name:
            name, ns = name.split(':')
            dest = self.namespaces[ns]
        dest[prefix + name] = value


class MultiParser:

    def __init__(self, spiders=None, cmd='tider crawl'):
        self.spiders = spiders or []
        self.cmd = cmd

    def parse(self, p):
        names = p.values
        options = dict(p.options)
        cmd = options.pop('--cmd', self.cmd)
        concurrency = options.pop('--worker-concurrency', '1')
        self._update_ns_opts(p, names)
        if not names:
            return (
                self._node_from_options(
                    p, name, cmd, options)
                for name in self.spiders
            )
        return (
            self._node_from_options(
                p, name, cmd, options)
            for name in names for _ in range(int(concurrency))
        )

    def _node_from_options(self, p, name, cmd, options):
        namespace = nodename = name
        return Node(nodename, cmd,
                    p.optmerge(namespace, options), p.passthrough)

    def _update_ns_opts(self, p, names):
        # Numbers in args always refers to the index in the list of names.
        # (e.g., `start foo bar baz -c:1` where 1 is foo, 2 is bar, and so on).
        for ns_name, ns_opts in list(p.namespaces.items()):
            if ns_name.isdigit():
                ns_index = int(ns_name) - 1
                if ns_index < 0:
                    raise KeyError(f'Indexes start at 1 got: {ns_name!r}')
                try:
                    p.namespaces[names[ns_index]].update(ns_opts)
                except IndexError:
                    raise KeyError(f'No node at index {ns_name!r}')

    def _update_ns_ranges(self, p, ranges):
        for ns_name, ns_opts in list(p.namespaces.items()):
            if ',' in ns_name or (ranges and '-' in ns_name):
                for subns in self._parse_ns_range(ns_name, ranges):
                    p.namespaces[subns].update(ns_opts)
                p.namespaces.pop(ns_name)

    def _parse_ns_range(self, ns, ranges=False):
        ret = []
        for space in ',' in ns and ns.split(',') or [ns]:
            if ranges and '-' in space:
                start, stop = space.split('-')
                ret.extend(
                    str(n) for n in range(int(start), int(stop) + 1)
                )
            else:
                ret.append(space)
        return ret


class Node:
    """Represents a node in a cluster."""

    def __init__(self, name,
                 cmd=None, append=None, options=None, extra_args=None):
        self.name = name
        self.cmd = cmd or f"-m {tider_exe('crawl', '--detach')}"
        if 'crawl' not in self.cmd:
            self.cmd += ' crawl --detach'
        self.append = append
        self.extra_args = extra_args or ''
        self.options = self._annotate_with_default_opts(
            options or OrderedDict())
        self.argv = self._prepare_argv()
        self._pid = None

    def _annotate_with_default_opts(self, options):
        options['-s'] = self.name
        self._setdefaultopt(options, ['--executable'], sys.executable)
        return options

    def _setdefaultopt(self, d, alt, value):
        for opt in alt[1:]:
            try:
                return d[opt]
            except KeyError:
                pass
        value = d.setdefault(alt[0], os.path.normpath(value))
        dir_path = os.path.dirname(value)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)
        return value

    def _prepare_argv(self):
        cmd = self.cmd.split(' ')
        i = cmd.index('tider') + 1
        options = self.options.copy()
        for opt, value in self.options.items():
            if opt in (
                '-s', '--spider',
                '--settings',
                '--schema',
                '--workdir',
                '-C', '--no-color',
                '-q', '--quiet',
            ):
                cmd.insert(i, format_opt(opt, value))

                options.pop(opt)

        cmd = [' '.join(cmd)]
        argv = tuple(
            cmd +
            [format_opt(opt, value)
             for opt, value in options.items()] +
            [self.extra_args]
        )
        return argv

    def start(self, env=None, **kwargs):
        return self._waitexec(
            self.argv, path=self.executable, env=env, **kwargs)

    def _waitexec(self, argv, path=sys.executable, env=None,
                  on_spawn=None, on_signalled=None, on_failure=None):
        argstr = self.prepare_argv(argv, path)
        maybe_call(on_spawn, self, argstr=' '.join(argstr), env=env)
        pipe = subprocess.Popen(argstr, env=env)
        return self.handle_process_exit(
            pipe.wait(),
            on_signalled=on_signalled,
            on_failure=on_failure,
        )

    def prepare_argv(self, argv, path):
        args = ' '.join([path] + list(argv))
        return shlex.split(from_utf8(args), posix=not IS_WINDOWS)

    def handle_process_exit(self, retcode, on_signalled=None, on_failure=None):
        if retcode < 0:
            maybe_call(on_signalled, self, -retcode)
            return -retcode
        elif retcode > 0:
            maybe_call(on_failure, self, retcode)
        return retcode

    @cached_property
    def executable(self):
        return self.options['--executable']


class Cluster(UserList):
    """Represent a cluster of spiders."""

    def __init__(self, nodes, cmd=None, env=None,
                 on_node_start=None, on_node_status=None,
                 on_child_spawn=None, on_child_failure=None):
        super().__init__(nodes)
        self.nodes = nodes
        self.cmd = cmd
        self.env = env
        self.on_node_start = on_node_start
        self.on_node_status = on_node_status
        self.on_child_spawn = on_child_spawn
        self.on_child_failure = on_child_failure

    def start(self):
        return [self.start_node(node) for node in self]

    def start_node(self, node):
        maybe_call(self.on_node_start, node)
        retcode = self._start_node(node)
        maybe_call(self.on_node_status, node, retcode)
        return retcode

    def _start_node(self, node):
        return node.start(self.env,
                          on_spawn=self.on_child_spawn,
                          on_failure=self.on_child_failure)


class TermLogger:

    #: Final exit code.
    retcode = 0

    def setup_terminal(self, stdout, stderr, quiet=False, verbose=False):
        self.stdout = stdout or sys.stdout
        self.stderr = stderr or sys.stderr
        self.quiet = quiet
        self.verbose = verbose

    def ok(self, m, newline=True, file=None):
        self.say(m, newline=newline, file=file)
        return EX_OK

    def say(self, m, newline=True, file=None):
        print(m, file=file or self.stdout, end='\n' if newline else '')

    def carp(self, m, newline=True, file=None):
        return self.say(m, newline, file or self.stderr)

    def error(self, msg=None):
        if msg:
            self.carp(msg)
        return EX_FAILURE

    def info(self, msg, newline=True):
        if self.verbose:
            self.note(msg, newline=newline)

    def note(self, msg, newline=True):
        if not self.quiet:
            self.say(str(msg), newline=newline)


class MultiTool(TermLogger):
    """The ``tider multi`` program."""

    def __init__(self, spiders, env=None, cmd=None, stdout=None, stderr=None, **kwargs):
        self.spiders = spiders
        self.env = env
        self.cmd = cmd or f"-m {TIDER_EXE}"
        self.setup_terminal(stdout, stderr, **kwargs)
        self.prog_name = 'tider multi'
        self.commands = {
            'start': self.start
        }

    def execute_from_commandline(self, argv, cmd=None):
        self.cmd = cmd if cmd is not None else self.cmd
        self.prog_name = os.path.basename(argv.pop(0))

        if not self.validate_arguments(argv):
            return self.error(f"Invalid arguments: {argv}")

        return self.call_command(argv[0], argv[1:])

    @staticmethod
    def validate_arguments(argv):
        return argv and argv[0][0] != '-'

    def call_command(self, command, argv):
        try:
            return self.commands[command](*argv) or EX_OK
        except KeyError:
            return self.error(f'Invalid command: {command}')

    @staticmethod
    def prepare_argv(argv, path):
        args = ' '.join([path] + list(argv))
        return shlex.split(args, posix=not IS_WINDOWS)

    def cluster_from_argv(self, argv, cmd=None):
        _, cluster = self._cluster_from_argv(argv, cmd=cmd)
        return cluster

    def _cluster_from_argv(self, argv, cmd=None):
        p, nodes = self._nodes_from_argv(argv, cmd=cmd)
        return p, Cluster(list(nodes), cmd=cmd,
                          on_node_start=self.on_node_start,
                          on_node_status=self.on_node_status,
                          on_child_spawn=self.on_child_spawn,
                          on_child_failure=self.on_child_failure)

    def _nodes_from_argv(self, argv, cmd=None):
        cmd = cmd if cmd is not None else self.cmd
        p = OptionParser(argv)
        p.parse()
        return p, MultiParser(spiders=self.spiders, cmd=cmd).parse(p)

    @using_cluster
    def start(self, cluster):
        self.note('> Starting spiders...')
        return int(any(cluster.start()))

    def on_node_start(self, node):
        self.note(f'\t> {node.name}: ', newline=False)

    def on_node_status(self, node, retval):
        self.note(retval and self.FAILED or self.OK)

    def on_child_spawn(self, node, argstr, env):
        self.info(f'  {argstr}')

    def on_child_failure(self, node, retcode):
        self.note(f'* Child terminated with exit code {retcode}')

    @property
    def FAILED(self):
        return click.style('FAILED', fg="red", bold=True)

    @property
    def OK(self):
        return click.style('OK', fg="green", bold=True)


@click.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True
    }
)
@click.pass_context
def multi(ctx):
    spiders = ctx.obj.tider.autodiscover_spiders()
    args = sys.argv[1:]
    if 'tider' not in args:
        args = ['-m', 'tider'] + args
    cmd = MultiTool(spiders=spiders, cmd=' '.join(args[:args.index('multi')]), quiet=ctx.obj.quiet)
    # rearrange the arguments so that the MultiTool will parse them correctly.
    args = args[args.index('multi'):]
    return cmd.execute_from_commandline(args)
