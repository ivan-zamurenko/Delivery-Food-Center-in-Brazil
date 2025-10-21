import sys
import os
from analysis.db_connection import get_engine


def main():
    #! Adjust the system path to include the parent directory
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    # ? Set up database engine
    engine = get_engine()


if __name__ == "__main__":
    main()
