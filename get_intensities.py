import sys
from datetime import datetime
from nmdb import entrypoint
print("started get_nmdb_intensities.sh at {} local time".format(str(datetime.now())))
sys.exit(entrypoint.main())
