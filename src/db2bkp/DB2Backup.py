from email.mime.text import MIMEText
import platform
import re
import os
import smtplib
import subprocess
import sys
import shutil
import datetime as dt
from collections import OrderedDict
from configparser import ConfigParser
import shlex
from subprocess import check_output, Popen, PIPE
import traceback

from db2bkp.utils.EnvInterpolation import EnvInterpolation


class DB2Backup():
    _LOGARCH_RE = re.compile('^[^()]+\s*(\(LOGARCHMETH\d+\))\s*=\s*DISK:(.*)$', re.MULTILINE)
    _ARCHPATHS_RE = re.compile('^\s*Path to log files\s*=\s*.*/(NODE\d{4})/(SQL\d{5})/(LOGSTREAM\d{4})/$', re.MULTILINE)
    _LOGFILE = re.compile('^S\d{7}.LOG$')

    def __init__(self, cfile, options):
        self._cfile = cfile
        self._options = options
        self._env = OrderedDict()
        self._fcfg = ConfigParser(allow_no_value=True, strict=False, delimiters='=')
        self._pcfg = ConfigParser(allow_no_value=True, strict=False, delimiters='=')
        self._bktime = dt.datetime.now()
        self._completed = False
        self._transfered = False
        self._tee = None
        self.rcode = 0
        self._init_env()

    def __enter__(self):
        return self

    def __exit__(self, etype, evalue, tr):
        try:
            self._env['Backup:completed'] = str(self._completed)
            self._flush_config()

            if self._completed:
                shutil.copy2(os.path.join(self._env['Backup:home'], 'backup.config'),
                             os.path.join(self._env['Backup:settings'], 'prev-backup.config'))
            self._cleanup()
        finally:
            if etype:
                self.rcode = 1
                traceback.print_exception(etype, evalue, tr, file=sys.stderr)
            self._flush_logs()
            self._notify()
            return self.rcode != 0

    def _notify(self):
        if 'Notifications' not in self._fcfg:
            return
        emails = self._fcfg.get('Notifications', 'emails', fallback=None)
        if not emails:
            return
        emails = re.split('\s*,\s*', emails)
        if len(emails) < 1:
            return
        logfile = os.path.join(self._env['Backup:settings'], 'backup%s.log' % self._env['Backup:timestamp'])
        with open(logfile) as f:
            msg = MIMEText(f.read().encode('utf-8'), _charset='utf-8')
            msg['Subject'] = 'DB2 BACKUP [%s] OF %s %s' % (
                self.get_backup_mode(), self._fcfg.get('Database', 'name'),
                ('COMPLETED' if self._completed else 'FAILED'))
            msg['From'] = self._fcfg.get('Notifications', 'from', fallback='%s@localhost' % self._env['User:home'])
            msg['To'] = ', '.join(emails)

        s = smtplib.SMTP(self._fcfg.get('Notifications', 'mxhost', fallback='localhost'))
        # s.set_debuglevel(1)
        s.send_message(msg)
        s.quit()

    def _flush_logs(self):
        if self._tee is not None:
            sys.stdout.flush()
            sys.stderr.flush()
            self._tee.stdin.flush()

    def _init_env(self):
        cfg = ConfigParser(interpolation=EnvInterpolation(self._env), allow_no_value=True, strict=False, delimiters='=')
        cfg.read(self._cfile)

        umask = cfg.get('System', 'umask', fallback='022')
        os.umask(int(umask, 8))

        output = check_output(['db2', 'get', 'instance'], universal_newlines=True)
        m = re.search('instance is:\s*([a-zA-Z0-9_\-]+)', output)
        if m is None or m.group(1) is None:
            raise Exception('Failure: %s' % output)

        self._env['Host:name'] = platform.node()
        self._env['User:home'] = os.path.expanduser('~')
        self._env['Backup:timestamp'] = self._bktime.strftime('%Y%m%d%H%M%S')
        self._env['Backup:dirname'] = cfg.get('Database', 'name') + '_' + self._env['Backup:timestamp']
        self._env['Backup:home'] = os.path.join(cfg.get('Paths', 'backup_parent_dir'),
                                                self._env['Backup:dirname'])
        self._env['Instance:name'] = m.group(1)

        sdir = os.path.join(os.path.expanduser('~'),
                            '.db2bkp',
                            self._env['Instance:name'],
                            cfg.get('Database', 'name'))
        print('BACKUP SETTINGS DIR %s' % sdir)
        if not os.path.exists(sdir):
            os.makedirs(sdir, exist_ok=True)
        self._ensure_dir_permissions(sdir, cfg)
        self._env['Backup:settings'] = sdir
        self._setup_logging(cfg)

        output = check_output(['db2greg', '-dump'], universal_newlines=True)
        # I,DB2,10.5.0.1,db2inst1,/home/db2inst1/sqllib,,1,0,/opt/ibm/db2/V10.5,,
        m = re.compile('^I,([^,]+),([^,]+),{0},([^,]+).*$'.format(self._env['Instance:name']),
                       re.MULTILINE).search(output)
        if m is None:
            raise Exception('Unable to find HOME of instenace: %s' % self._env['Instance:name'])
        self._env['Instance:home'] = os.path.dirname(m.group(3))
        self._env['DB2:version'] = m.group(2)

        sout, _ = self._send_cmd(['db2', '-t'],
                                 """
                                 CONNECT TO %s;
                                 GET DB CFG;
                                 """
                                 % (cfg.get('Database', 'name')))

        # Path to log files    = /home/db2inst1/db2inst1/NODE0000/SQL00001/LOGSTREAM0000/
        m = self._ARCHPATHS_RE.search(sout)
        if m is None:
            raise Exception(
                'Database: %s is not configured with logging' % cfg.get('Database', 'name'))
        self._env['Database:node'] = m.group(1)
        self._env['Database:sql'] = m.group(2)
        self._env['Database:logstream'] = m.group(3)

        # First log archive method                 (LOGARCHMETH1) = DISK:/home/db2inst1/backup/
        m = self._LOGARCH_RE.search(sout)
        if m is None:
            raise Exception(
                'Database: %s is not configured with any DISK LOGARCH method' % cfg.get('Database', 'name'))

        lapath = os.path.join(m.group(2),
                              self._env['Instance:name'],
                              cfg.get('Database', 'name'),
                              self._env['Database:node'],
                              self._env['Database:logstream'])
        lapath = os.path.join(lapath, os.listdir(lapath)[-1])
        self._env['Database:archlogs'] = lapath

        fdict = {}
        for sec in cfg.sections():
            fdict[sec] = {}
            for opt, _ in cfg[sec].items():
                fdict[sec][opt] = cfg[sec].get(opt)
        self._fcfg.read_dict(fdict)
        self._fcfg.add_section('Environment')

    def backup(self):
        self._load_prev_cfg()
        self._env['Backup:full_timestamp'] = self._pcfg.get('Environment', 'backup:full_timestamp', fallback=None)
        if self._is_fullbackup_required():
            self._env['Backup:mode'] = 'Full'
            self._env['Backup:full_timestamp'] = self._env['Backup:timestamp']
        else:
            self._env['Backup:mode'] = 'Logs'

        print('ENVIRONMENT:\n' + '\n'.join('{0}: {1}'.format(k, v) for k, v in self._env.items()))
        self._flush_config()
        self._full_backup()
        self._logs_backup()
        self._flush_logs()
        self._transfer()
        self._completed = True

    def get_backup_mode(self):
        return self._env.get('Backup:mode')

    def _is_fullbackup_required(self):
        # If previous backup exists
        if len(self._pcfg) < 2:
            print('FULL BACKUP REQUIRED: PREVIOUS BACKUP NOT EXISTS')
            return True

        # Time diff from last backup
        last_backup_older = self._to_timedelta(self._fcfg.get('FullBackupWhen', 'last_backup_older', fallback=None))
        prev_ts = self._pcfg.get('Environment', 'backup:timestamp', fallback=None)
        if last_backup_older \
                and prev_ts \
                and self._bktime >= last_backup_older + dt.datetime.strptime(prev_ts, '%Y%m%d%H%M%S'):
            print('FULL BACKUP REQUIRED: LAST BACKUP OLDER THAN: %s' % last_backup_older)
            return True

        last_backup_older = self._to_timedelta(self._fcfg.get('FullBackupWhen', 'full_backup_older', fallback=None))
        prev_ts = self._env.get('Backup:full_timestamp')
        if last_backup_older \
                and (prev_ts is None or self._bktime >= last_backup_older + dt.datetime.strptime(prev_ts,
                                                                                                 '%Y%m%d%H%M%S')):
            print('FULL BACKUP REQUIRED: LAST FULL BACKUP OLDER THAN: %s' % last_backup_older)
            return True

        # Check number of logs
        logs = [x for x in os.listdir(self._env['Database:archlogs']) if re.match(self._LOGFILE, x)]
        max_nlogs = self._fcfg.getint('FullBackupWhen', 'number_of_logs_more', fallback=sys.maxsize)
        if max_nlogs < len(logs):
            print('FULL BACKUP REQUIRED: NUMBER OF LOGS: %d EXCEEDED THE LIMIT: %d' % (len(logs), max_nlogs))
            return True

        print('NO FULL BACKUP')
        return False

    @staticmethod
    def _to_timedelta(spec):
        if spec is None:
            return None
        spec = re.split('\s+', spec)
        units = spec[-1] if len(spec) > 1 else 'days'
        if units not in ['days', 'seconds', 'weeks', 'hours']:
            raise Exception('Invalid timespec: %s' % spec)
        return dt.timedelta(**{
            units: int(spec[0])
        })

    def _full_backup(self):
        if self._env['Backup:mode'] != 'Full':
            return
        sout, serr = self._send_cmd(['db2', '-t'], '%s' % self._fcfg.get('Execution', 'backup'))
        print(sout)
        print(serr)

    def _logs_backup(self):
        self._send_cmd(['db2', '-t'],
                       '%s' % self._fcfg.get('Execution', 'prepare_logs'))

        logs = [x for x in os.listdir(self._env['Database:archlogs']) if re.match(self._LOGFILE, x)]
        logs.sort()
        print("ACTUAL LOG FILES: %s" % logs)

        lastlog = self._pcfg.get('Environment', 'backup:lastlog', fallback=None)
        if lastlog in logs:
            logs = logs[logs.index(lastlog) + 1:]

        print('LAST LOG: %s' % lastlog)
        for l in logs:
            print('LOGFILE: %s STORED' % l)
            lpath = os.path.join(self._env['Database:archlogs'], l)
            shutil.copy2(lpath, os.path.join(self._env['Backup:home'], l))

        if len(logs) > 0:
            print('CURRENT Backup:lastlog=%s' % logs[-1])
            self._env['Backup:lastlog'] = logs[-1]

    def _transfer(self):
        script = self._fcfg.get('Transfer', 'script', fallback=None)
        if not script:
            return
        sout, serr = self._send_cmd([script, self._env['Backup:home'], self._env['Backup:mode']])
        print(sout)
        print(serr)
        self._transfered = True

    def _cleanup(self):
        if self._completed:
            self._cleanup_completed()

    def _cleanup_completed(self):
        prune = self._fcfg.getboolean('Cleanup', 'prune_logs', fallback=False)
        if not prune:
            prune = (self._fcfg.getboolean('Cleanup', 'prune_logs_if_transfered', fallback=False)
                     and self._transfered)
        if not prune:
            prune = (self._fcfg.getboolean('Cleanup', 'prune_logs_if_full', fallback=False)
                     and self._transfered
                     and self._env['Backup:mode'] == 'Full')
        if prune:
            print('PRUNE HISTORY %s AND DELETE' % self._env['Backup:timestamp'])
            self._send_cmd(['db2', '-t'], """
                    CONNECT TO %s;
                    PRUNE HISTORY %s AND DELETE;
                    CONNECT RESET;
                    """ % (self._fcfg.get('Database', 'name'), self._env['Backup:timestamp']))

        cleanup_older_than = self._to_timedelta(self._fcfg.get('Cleanup', 'cleanup_older_than', fallback=None))
        if cleanup_older_than:
            backup_parent_dir = self._fcfg.get('Paths', 'backup_parent_dir')
            bdir_re = re.compile('^%s_(\d{14})$' % re.escape(self._fcfg.get('Database', 'name')))
            for bdir in [x for x in os.listdir(backup_parent_dir) if os.path.isdir(os.path.join(backup_parent_dir, x))]:
                m = bdir_re.search(bdir)
                if m is None:
                    continue
                btime = dt.datetime.strptime(m.group(1), '%Y%m%d%H%M%S')
                if btime + cleanup_older_than < self._bktime:
                    print("REMOVE OLD BACKUP TREE: %s OLDER THAN %s" %
                          (os.path.join(backup_parent_dir, bdir), cleanup_older_than))
                    shutil.rmtree(os.path.join(backup_parent_dir, bdir))

    def _flush_config(self):
        for k, v in self._env.items():
            self._fcfg.set('Environment', k, v)
        lbdir = self._env['Backup:home']
        with open(os.path.join(lbdir, 'backup.config'), 'w') as cfile:
            self._fcfg.write(cfile)
        ppath = os.path.join(self._env['Backup:settings'], 'prev-backup.config')
        if os.path.exists(ppath):
            shutil.copy2(ppath, os.path.join(self._env['Backup:home'],
                                             'prev-backup.config'))
            shutil.copy2(ppath, os.path.join(self._env['Backup:settings'],
                                             'backup%s.config' % self._env['Backup:timestamp']))

    def _load_prev_cfg(self):
        ppath = os.path.join(self._env['Backup:settings'], 'prev-backup.config')
        if os.path.exists(ppath):
            pcfg = ConfigParser(allow_no_value=True, strict=False, delimiters='=')
            pcfg.read(ppath)
            if pcfg.get('Environment', 'backup:completed', fallback=False):
                self._pcfg = pcfg


    @staticmethod
    def _send_cmd(args, indata=None):
        if isinstance(args, str):
            args = shlex.split(args)
        print(' '.join(args) + ' ' + (indata if indata else ''))
        penv = {}
        penv.update(os.environ)
        with Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=penv, universal_newlines=True) as sp:
            ret = sp.communicate(input=indata)
            if sp.returncode:
                raise Exception('\n\n\nCOMMAND FAILED: %s '
                                '\nSTDOUT:\n%s '
                                '\nSTDERR:\n%s\n\n' % (' '.join(args), ret[0], ret[1]))
        return ret[0], ret[1]

    @staticmethod
    def _ensure_dir_permissions(dir, cfg):
        dirgroup = cfg.get('System', 'dirgroup', fallback=None)
        if dirgroup and shutil.chown:
            shutil.chown(dir, group=dirgroup)

    def _setup_logging(self, cfg):
        for d in ['Backup:settings', 'Backup:home']:
            d = self._env[d]
            if not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
            self._ensure_dir_permissions(d, cfg)

        cmd = ['tee',
               os.path.join(self._env['Backup:settings'], 'backup%s.log' % self._env['Backup:timestamp']),
               os.path.join(self._env['Backup:home'], 'backup.log')]
        self._tee = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        if self._tee.returncode:
            raise Exception('\n\n Command failed: %s %s %s' % (' '.join(cmd), self._tee.stdout, self._tee.stderr))
        os.dup2(self._tee.stdin.fileno(), sys.stdout.fileno())
        os.dup2(self._tee.stdin.fileno(), sys.stderr.fileno())


