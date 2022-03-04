import os
import sys

import logging
SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
PACKAGE_PATH = os.path.abspath(os.path.join(SCRIPT_PATH,".."))

sys.path.append(PACKAGE_PATH)
import settings
from {{package_name}} import {{class_name}}

level = logging.INFO
if 'LOGGING_DEBUG' in locals():
    if settings.LOGGING_DEBUG:
        level=logging.DEBUG

logging.basicConfig(level=level,
                    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',)


def main():
    step = {{class_name}}(config=settings.STEP_CONFIG,level=level)
    step.start()
    sys.path.remove(PACKAGE_PATH)

if __name__ == "__main__":
    main()
