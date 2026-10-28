import sys
from pathlib import Path

NOTEBOOKS = Path(__file__).resolve().parents[2] / "src" / "notebooks"
sys.path.insert(0, str(NOTEBOOKS))
