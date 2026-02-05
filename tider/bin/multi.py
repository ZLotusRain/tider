"""Start multiple spiders from the command-line.

.. program:: tider multi

Examples
========

.. code-block:: console

    $ # Single spider with explicit name.
    $ tider multi start foo

    $ # 3 processes
    $ tider --settings=settings --schema=schema -s foo multi start 3

    $ # You can show the commands necessary to start the crawler with
    $ # the 'show' command:
    $ tider -s foo multi show -l INFO
"""
import errno
import os
import shlex
import signal
import sys
import click
import subprocess
from time import sleep
from functools import wraps, partial
from kombu.utils.encoding import from_utf8
from kombu.utils.objects import cached_property
from collections import OrderedDict, UserList, defaultdict

from tider import __version__
from tider.platforms import (
    EX_FAILURE,
    EX_OK,
    IS_WINDOWS,
    PidDirectory,
    Pidfile,
    signals,
    signal_name
)
from tider.utils import term
from tider.utils.text import pluralize
from tider.utils.nodenames import node_format
from tider.utils.saferepr import saferepr

try:
    from importlib.metadata import distribution, distributions
except ModuleNotFoundError:
    from importlib_metadata import distribution, distributions

# tricky way to decide how to start a tider program.
INSTALLED_PACKAGES = [dist.metadata["Name"] for dist in distributions() if "Name" in dist.metadata]
TIDER_EXE = 'tider' if 'tider' in INSTALLED_PACKAGES else '-m tider'

del INSTALLED_PACKAGES


USAGE = """\
usage: {prog_name} start <spider1 spider2 spiderN|range> [crawl options]
       {prog_name} stop <spider1 spider2 spiderN|range> [-SIG (default: -TERM)]
       {prog_name} restart <spider1 spider2 spiderN|range> [-SIG] [crawl options]
       {prog_name} kill <spider1 spider2 spiderN|range>

       {prog_name} show <spider1 spider2 spiderN|range> [crawl options]
       {prog_name} get hostname <spider1 spider2 spiderN|range> [-qv] [crawl options]
       {prog_name} names <spider1 spider2 spiderN|range>
       {prog_name} expand template <spider1 spider2 spiderN|range>
       {prog_name} help

additional options (must appear after command name):

    * --nosplash:   Don't display program info.
    * --quiet:      Don't show as much output.
    * --verbose:    Show more output.
    * --no-color:   Don't display colors.
"""


def tider_exe(*args):
    return ' '.join((TIDER_EXE,) + args)


def build_expander(nodename, shortname, hostname, group):
    return partial(
        node_format,
        name=nodename,
        N=shortname,
        d=hostname,
        h=nodename,
        i='%i',
        I='%I',
        g=group
    )


def maybe_call(fun, *args, **kwargs):
    if fun is not None:
        fun(*args, **kwargs)


def format_opt(opt, value):
    if not value:
        return opt
    if opt.startswith('--'):
        return f'{opt}={value}'
    return f'{opt} {value}'


def splash(fun):

    @wraps(fun)
    def _inner(self, *args, **kwargs):
        self.splash()
        return fun(self, *args, **kwargs)
    return _inner


def using_cluster(fun):

    @wraps(fun)
    def _inner(self, *argv, **kwargs):
        return fun(self, self.cluster_from_argv(argv), **kwargs)
    return _inner


def using_cluster_and_sig(fun):

    @wraps(fun)
    def _inner(self, *argv, **kwargs):
        p, cluster = self._cluster_from_argv(argv)
        sig = self._find_sig_argument(p)
        return fun(self, cluster, sig, **kwargs)
    return _inner


class NamespacedOptionParser:

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
                    value = None
                    if '=' not in arg[2:]:
                        # process options like ['--proj', 'default']
                        if len(rargs) > pos + 1 and rargs[pos + 1][0] != '-':
                            value = rargs[pos + 1]
                            pos += 1
                    self.process_long_opt(arg[2:], value=value)
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

    def __init__(self, app, schemas=None, cmd='tider crawl', append='', prefix='', suffix='',
                 range_prefix='tider'):
        self.app = app
        self.schemas = schemas or {}
        self.cmd = cmd
        self.append = append
        self.prefix = prefix
        self.suffix = suffix
        self.range_prefix = range_prefix

    def parse(self, p):
        names = p.values
        options = dict(p.options)
        name = options.pop('-s', None) or options.pop('--spider', None)
        if not names and name:
            names = [name]
        ranges = len(names) == 1
        cmd = options.pop('--cmd', self.cmd)
        # concurrency = options.pop('--worker-concurrency', '1')
        if ranges:
            try:
                concurrency = int(names[0])
                names = [name for _ in range(concurrency)]
                options.setdefault('-dup', None)
            except ValueError:
                pass
        self._update_ns_opts(p, names)
        if not names:
            return (
                self._node_from_options(
                    p, name, schema, cmd, dict(options))
                for schema in self.schemas for name in self.schemas[schema]
            )
        return (
            self._node_from_options(
                p, name, schema, cmd, dict(options))
            # matched spiders
            for name in names for schema in self.schemas if name in self.schemas[schema]
        )

    def _node_from_options(self, p, spider, schema, cmd, options):
        namespace = spider  # spider name
        return Node(app=self.app, spider=spider, schema=schema, cmd=cmd,
                    options=p.optmerge(namespace, options), extra_args=p.passthrough)

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

    def __init__(self, app, spider, schema='default',
                 cmd=None, append=None, options=None, extra_args=None):
        self.app = app
        self.spider = spider
        self.schema = schema
        self.cmd = cmd or tider_exe('crawl', '--detach')
        if 'crawl' not in self.cmd:
            self.cmd += ' crawl --detach'
        self.append = append
        self.extra_args = extra_args or ''
        self.options = options  # avoid options loss
        self.options = self._annotate_with_default_opts(
            options or OrderedDict())
        self.argv = self._prepare_argv()

    @cached_property
    def crawler(self):
        try:
            if self.options.get('--data-source'):
                transport = 'files'
            else:
                transport = self.options.get('-b') or self.options.get('--transport')
            return self.app.Crawler(
                self.app.load(self.spider, schema=self.schema),
                schema=self.schema,
                hostname=self.options.get('-n') or self.options.get('--hostname'),
                broker_transport=transport,
                allow_duplicates=self.options.get('-dup') or self.options.get('--allow-duplicates'),
            )
        except KeyError as e:
            # spider not found.
            raise ValueError(e)

    @property
    def name(self):
        # name without unique pid.
        return self.crawler.group

    @cached_property
    def expander(self):
        return self._prepare_expander()

    def _annotate_with_default_opts(self, options):
        if not self.spider:
            raise ValueError('Expect valid spider name, but no spider provided')
        options['-s'] = self.spider
        if self.schema and self.schema != 'default':
            options.setdefault('--schema', self.schema)  # specific schema
        self._setdefaultopt(options, ['--pidfile', '-p'], f'/var/run/tider/%g/%n.pid')
        # self._setdefaultopt(options, ['--logfile', '-f'], '/var/log/tider/%n%I.log')
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
        if dir_path:
            dir_path = dir_path.split('/%g')[0]  # don't create group.
            if '%' in dir_path:
                dir_path = self.expander(dir_path)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
        return value

    def _prepare_expander(self):
        shortname, hostname = self.name, None
        if '@' in self.name:
            shortname, hostname = self.name.split('@', 1)
        return build_expander(
            self.name, shortname, hostname, group=self.crawler.group)

    def _prepare_argv(self):
        cmd = self.cmd.split(' ')
        i = cmd.index('tider') + 1
        options = self.options.copy()
        for opt, value in self.options.items():
            if opt in (
                '-s', '--spider',
                '--settings',
                '--proj',
                '--schema',
                '--workdir',
                '-C', '--no-color',
                '-q', '--quiet',
            ):
                cmd.insert(i, format_opt(opt, value))

                options.pop(opt)
        for opt in self.options:
            # remove signal option.
            if not opt.startswith('-'):
                continue
            try:
                int(opt[1:])
            except ValueError:
                try:
                    signals.signum(opt[1:])
                except (AttributeError, TypeError):
                    continue
            options.pop(opt)

        cmd = [' '.join(cmd)]
        argv = tuple(
            cmd +
            [format_opt(opt, value)
             for opt, value in options.items()] +
            [self.extra_args]
        )
        return argv

    def alive(self, pid=None):
        return self.send(0, pid=pid)

    def send(self, sig, on_error=None, pid=None):
        if pid:
            try:
                os.kill(pid, sig)
            except OSError as exc:
                if exc.errno != errno.ESRCH:
                    raise
                maybe_call(on_error, self, pid=pid)
                return False
            return True
        maybe_call(on_error, self)

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

    def getopt(self, *alt):
        for opt in alt:
            try:
                return self.options[opt]
            except KeyError:
                pass
        raise KeyError(alt[0])

    def __repr__(self):
        return f'<{type(self).__name__}: {self.name}>'

    @cached_property
    def piddirectory(self):
        if os.path.dirname(self.pidfile).split('/')[-1] == self.name:
            return os.path.dirname(self.pidfile)

    @cached_property
    def pidfile(self):
        return self.expander(self.getopt('--pidfile', '-p'))

    @cached_property
    def logfile(self):
        return self.expander(self.getopt('--logfile', '-f'))

    @property
    def pids(self):
        try:
            if self.piddirectory:
                pids = PidDirectory(self.piddirectory).read_pids()
            else:
                pid = Pidfile(self.pidfile).read_pid()
                pids = [pid] if pid else []
            return pids
        except ValueError:
            pass

    @cached_property
    def executable(self):
        return self.options['--executable']

    @cached_property
    def argv_with_executable(self):
        return (self.executable,) + self.argv


class Cluster(UserList):
    """Represent a cluster of spiders."""

    def __init__(self, nodes, cmd=None, env=None,
                 on_stopping_preamble=None,
                 on_send_signal=None,
                 on_still_waiting_for=None,
                 on_still_waiting_progress=None,
                 on_still_waiting_end=None,
                 on_node_start=None,
                 on_node_restart=None,
                 on_node_shutdown_ok=None,
                 on_node_status=None,
                 on_node_signal=None,
                 on_node_signal_dead=None,
                 on_node_down=None,
                 on_child_spawn=None,
                 on_child_signalled=None,
                 on_child_failure=None):
        super().__init__(nodes)
        self.nodes = nodes
        self.cmd = cmd or tider_exe('crawl', '--detach')
        self.env = env

        self.on_stopping_preamble = on_stopping_preamble
        self.on_send_signal = on_send_signal
        self.on_still_waiting_for = on_still_waiting_for
        self.on_still_waiting_progress = on_still_waiting_progress
        self.on_still_waiting_end = on_still_waiting_end
        self.on_node_start = on_node_start
        self.on_node_restart = on_node_restart
        self.on_node_shutdown_ok = on_node_shutdown_ok
        self.on_node_status = on_node_status
        self.on_node_signal = on_node_signal
        self.on_node_signal_dead = on_node_signal_dead
        self.on_node_down = on_node_down
        self.on_child_spawn = on_child_spawn
        self.on_child_signalled = on_child_signalled
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
                          on_signalled=self.on_child_signalled,
                          on_failure=self.on_child_failure)

    def send_all(self, sig):
        for node, pids in self.getpids(on_down=self.on_node_down).items():
            for pid in pids:
                maybe_call(self.on_node_signal, node, signal_name(sig), pid=pid)
                node.send(sig, self.on_node_signal_dead, pid=pid)
            if node.piddirectory:
                PidDirectory(node.piddirectory).remove_if_stale()

    def kill(self):
        return self.send_all(signal.SIGKILL)

    def restart(self, sig=signal.SIGTERM):
        retvals = []

        def restart_on_down(node):
            maybe_call(self.on_node_restart, node)
            retval = self._start_node(node)
            maybe_call(self.on_node_status, node, retval)
            retvals.append(retval)

        self._stop_nodes(retry=2, on_down=restart_on_down, sig=sig)
        return retvals

    def stop(self, retry=None, callback=None, sig=signal.SIGTERM):
        return self._stop_nodes(retry=retry, on_down=callback, sig=sig)

    def stopwait(self, retry=2, callback=None, sig=signal.SIGTERM):
        return self._stop_nodes(retry=retry, on_down=callback, sig=sig)

    def _stop_nodes(self, retry=None, on_down=None, sig=signal.SIGTERM):
        on_down = on_down if on_down is not None else self.on_node_down
        nodes = [node for node in self.getpids(on_down=on_down)]
        if nodes:
            for node in self.shutdown_nodes(nodes, sig=sig, retry=retry):
                maybe_call(on_down, node)

    def shutdown_nodes(self, nodes, sig=signal.SIGTERM, retry=None):
        P = set(nodes)
        maybe_call(self.on_stopping_preamble, nodes)
        to_remove = set()
        processed = set()
        for node in P:
            # same group
            names = [n.name for n in list(to_remove)]
            if node.name in names:
                to_remove.add(node)
                yield node
                continue
            names = [n.name for n in list(processed)]
            if node.name in names:
                continue

            processed.add(node)
            if len(node.pids) > 1:
                node.options.setdefault('-dup', True)  # restart with multiple processes
            stopped = True
            for pid in node.pids:
                maybe_call(self.on_send_signal, node, signal_name(sig), pid=pid)
                if node.send(sig, self.on_node_signal_dead, pid=pid):
                    # process still running
                    stopped = False
            if stopped:
                to_remove.add(node)
                yield node
        P -= to_remove
        if retry:
            maybe_call(self.on_still_waiting_for, P)
            its = 0
            to_remove_all = set()
            while P:
                to_remove = set()
                processed = set()
                for node in P:
                    # same group
                    names = [n.name for n in list(to_remove_all)]
                    if node.name in names:
                        maybe_call(self.on_node_shutdown_ok, node)
                        to_remove.add(node)
                        yield node
                        maybe_call(self.on_still_waiting_for, P)
                        break
                    names = [n.name for n in list(processed)]
                    if node.name in names:
                        continue

                    processed.add(node)

                    its += 1
                    maybe_call(self.on_still_waiting_progress, P)
                    if all([not node.alive(pid=pid) for pid in node.pids]):
                        maybe_call(self.on_node_shutdown_ok, node)
                        to_remove.add(node)
                        to_remove_all.add(node)
                        yield node
                        maybe_call(self.on_still_waiting_for, P)
                        break
                P -= to_remove
                if P and not its % len(P):
                    sleep(float(retry))
            maybe_call(self.on_still_waiting_end)

    def find(self, name):
        for node in self:
            if node.name == name:
                return node
        raise KeyError(name)

    def getpids(self, on_down=None):
        result = {}
        for node in self:
            pids = node.pids
            names = [n.name for n in result]
            if node.name in names:
                result[node] = []  # same group with the same pids
            elif pids:
                result[node] = pids
                names.append(node.name)
            else:
                maybe_call(on_down, node)
        return result

    def __repr__(self):
        return '<{name}({0}): {1}>'.format(
            len(self), saferepr([n.name for n in self]),
            name=type(self).__name__,
        )


class TermLogger:

    splash_text = 'tider multi v{version}'
    splash_context = {'version': __version__}

    #: Final exit code.
    retcode = 0

    def setup_terminal(self, stdout, stderr, nosplash=False, quiet=False,
                       verbose=False, no_color=False):
        self.stdout = stdout or sys.stdout
        self.stderr = stderr or sys.stderr
        self.nosplash = nosplash
        self.quiet = quiet
        self.verbose = verbose
        self.no_color = no_color

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
        self.usage()
        return EX_FAILURE

    def info(self, msg, newline=True):
        if self.verbose:
            self.note(msg, newline=newline)

    def note(self, msg, newline=True):
        if not self.quiet:
            self.say(str(msg), newline=newline)

    @splash
    def usage(self):
        self.say(USAGE.format(prog_name=self.prog_name))

    def splash(self):
        if not self.nosplash:
            self.note(self.colored.cyan(
                self.splash_text.format(**self.splash_context)))

    @cached_property
    def colored(self):
        return term.colored(enabled=not self.no_color)


class MultiTool(TermLogger):
    """The ``tider multi`` program."""

    reserved_options = [
        ('--nosplash', 'nosplash'),
        ('--quiet', 'quiet'),
        ('-q', 'quiet'),
        ('--verbose', 'verbose'),
        ('--no-color', 'no_color'),
    ]

    def __init__(self, app, schema=None, env=None, cmd=None,
                 stdout=None, stderr=None, **kwargs):
        self.app = app
        result = app.autodiscover_spiders()
        if not schema:
            schemas = result.copy()
        else:
            schemas = {schema: result[schema]}
        self.schemas = schemas

        self.env = env
        self.cmd = cmd
        self.setup_terminal(stdout, stderr, **kwargs)
        self.prog_name = 'tider multi'
        self.commands = {
            'start': self.start,
            'show': self.show,
            'stop': self.stop,
            'stopwait': self.stopwait,
            'restart': self.restart,
            'kill': self.kill,
            'names': self.names,
            'expand': self.expand,
            'get': self.get,
            'help': self.help,
        }

    def execute_from_commandline(self, argv, cmd=None):
        # Reserve the --nosplash|--quiet|-q/--verbose options.
        argv = self._handle_reserved_options(argv)
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

    def _handle_reserved_options(self, argv):
        argv = list(argv)  # don't modify callers argv.
        for arg, attr in self.reserved_options:
            if arg in argv:
                setattr(self, attr, bool(argv.pop(argv.index(arg))))
        return argv

    @splash
    @using_cluster
    def start(self, cluster):
        self.note('> Starting spiders...')
        return int(any(cluster.start()))

    @splash
    @using_cluster_and_sig
    def stop(self, cluster, sig, **kwargs):
        return cluster.stop(sig=sig, **kwargs)

    @splash
    @using_cluster_and_sig
    def stopwait(self, cluster, sig, **kwargs):
        return cluster.stopwait(sig=sig, **kwargs)

    @splash
    @using_cluster_and_sig
    def restart(self, cluster, sig, **kwargs):
        return int(any(cluster.restart(sig=sig, **kwargs)))

    @using_cluster
    def names(self, cluster):
        self.say('\n'.join(n.name for n in cluster))

    def get(self, wanted, *argv):
        try:
            node = self.cluster_from_argv(argv).find(wanted)
        except KeyError:
            return EX_FAILURE
        else:
            return self.ok(' '.join(node.argv))

    @using_cluster
    def show(self, cluster):
        return self.ok('\n'.join(
            ' '.join(node.argv_with_executable)
            for node in cluster
        ))

    @splash
    @using_cluster
    def kill(self, cluster):
        return cluster.kill()

    def expand(self, template, *argv):
        return self.ok('\n'.join(
            node.expander(template)
            for node in self.cluster_from_argv(argv)
        ))

    def help(self, *argv):
        self.say(__doc__)

    def _find_sig_argument(self, p, default=signal.SIGTERM):
        args = p.args[len(p.values):]
        for arg in reversed(args):
            if len(arg) == 2 and arg[0] == '-':
                try:
                    return int(arg[1])
                except ValueError:
                    pass
            if arg[0] == '-':
                try:
                    return signals.signum(arg[1:])
                except (AttributeError, TypeError):
                    pass
        return default

    def _nodes_from_argv(self, argv, cmd=None):
        cmd = cmd if cmd is not None else self.cmd
        p = NamespacedOptionParser(argv)
        p.parse()
        return p, MultiParser(app=self.app, schemas=self.schemas, cmd=cmd).parse(p)

    def cluster_from_argv(self, argv, cmd=None):
        _, cluster = self._cluster_from_argv(argv, cmd=cmd)
        return cluster

    def _cluster_from_argv(self, argv, cmd=None):
        p, nodes = self._nodes_from_argv(argv, cmd=cmd)
        return p, Cluster(list(nodes), cmd=cmd, env=self.env,
                          on_stopping_preamble=self.on_stopping_preamble,
                          on_send_signal=self.on_send_signal,
                          on_still_waiting_for=self.on_still_waiting_for,
                          on_still_waiting_progress=self.on_still_waiting_progress,
                          on_still_waiting_end=self.on_still_waiting_end,
                          on_node_start=self.on_node_start,
                          on_node_restart=self.on_node_restart,
                          on_node_shutdown_ok=self.on_node_shutdown_ok,
                          on_node_status=self.on_node_status,
                          on_node_signal_dead=self.on_node_signal_dead,
                          on_node_signal=self.on_node_signal,
                          on_node_down=self.on_node_down,
                          on_child_spawn=self.on_child_spawn,
                          on_child_signalled=self.on_child_signalled,
                          on_child_failure=self.on_child_failure,)

    def on_stopping_preamble(self, nodes):
        self.note(self.colored.blue('> Stopping nodes...'))

    def on_send_signal(self, node, sig, pid=None):
        self.note('\t> {0.name}: {1} -> {pid}'.format(node, sig, pid=pid))

    def on_still_waiting_for(self, nodes):
        num_left = len(nodes)
        if num_left:
            self.note(self.colored.blue(
                '> Waiting for {} {} -> {}...'.format(
                    num_left, pluralize(num_left, 'node'),
                    ', '.join(str(pid) for node in nodes for pid in node.pids if not node.piddirectory)),
            ), newline=False)

    def on_still_waiting_progress(self, nodes):
        self.note('.', newline=False)

    def on_still_waiting_end(self):
        self.note('')

    def on_node_signal_dead(self, node, pid):
        self.note(
            'Could not signal {0.name} ({pid}): No such process'.format(
                node, pid=pid))

    def on_node_start(self, node):
        self.note(f'\t> {node.name}: ', newline=False)

    def on_node_restart(self, node):
        self.note(self.colored.blue(
            f'> Restarting node {node.name}: '), newline=False)

    def on_node_down(self, node):
        self.note(f'> {node.name}: {self.DOWN}')

    def on_node_shutdown_ok(self, node):
        self.note(f'\n\t> {node.name}: {self.OK}')

    def on_node_status(self, node, retval):
        self.note(retval and self.FAILED or self.OK)

    def on_node_signal(self, node, sig, pid):
        self.note('Sending {sig} to node {0.name} ({pid})'.format(
            node, sig=sig, pid=pid))

    def on_child_spawn(self, node, argstr, env):
        self.info(f'  {argstr}')

    def on_child_signalled(self, node, signum):
        self.note(f'* Child was terminated by signal {signum}')

    def on_child_failure(self, node, retcode):
        self.note(f'* Child terminated with exit code {retcode}')

    @property
    def FAILED(self):
        return click.style('FAILED', fg="red", bold=True)

    @property
    def OK(self):
        return click.style('OK', fg="green", bold=True)

    @cached_property
    def DOWN(self):
        return str(self.colored.magenta('DOWN'))


@click.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True
    }
)
@click.pass_context
def multi(ctx):
    cmd = MultiTool(app=ctx.obj.app, schema=ctx.obj.schema, quiet=ctx.obj.quiet, no_color=ctx.obj.no_color)
    # rearrange the arguments so that the MultiTool will parse them correctly.
    args = sys.argv[1:]
    args = args[args.index('multi'):] + args[:args.index('multi')]
    return cmd.execute_from_commandline(args)
