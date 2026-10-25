import os
import sys

# Add project root directory to path so main.py and local modules are imported cleanly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app
