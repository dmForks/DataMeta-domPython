
# see https://docs.python.org/3/library/pkgutil.html
# without this, Python will have trouble finding packages that share some common tree off the root
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

