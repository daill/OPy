import logging
import sys

__author__ = 'daill'

# set logging properties
handler = logging.StreamHandler(sys.stdout)
frm = logging.Formatter("%(asctime)s %(filename)s line: %(lineno)d %(levelname)s: %(message)s", "%d.%m.%Y %H:%M:%S")
handler.setFormatter(frm)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)