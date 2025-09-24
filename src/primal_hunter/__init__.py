from gettext import install
from dotenv import load_dotenv
from rich.traceback import install as tr_install
from rich_color_ext import install as rc_install

from primal_hunter.logger import get_console, get_logger, get_progress
from primal_hunter.scripts.stylesheet import install_css_rich_console

_console = get_console()
progress = get_progress(_console)
log = get_logger(console=_console)

rc_install()
tr_install(console=_console)
load_dotenv()
install_css_rich_console()
