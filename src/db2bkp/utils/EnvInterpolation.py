import re
from configparser import ExtendedInterpolation


class EnvInterpolation(ExtendedInterpolation):
    def __init__(self, env):
        super().__init__()
        self._env = env if isinstance(env, dict) else None

    def _interpolate_some(self, parser, option, accum, rest, section, map, depth):
        super()._interpolate_some(parser, option, accum, self._process_value(rest), section, map, depth)

    def _process_value(self, value):
        return value if value is None else \
            re.sub(self._KEYCRE, lambda m: self._env[m.group(1)] if m.group(1) in self._env else m.group(0), value)