"""
MongoDB writing test
"""
from __future__ import annotations

import sys
from logging import Logger, getLogger, NullHandler
from typing import Sequence, Mapping, Any

from Fundamental import Error
from MongoDb import MongoDB, make_mongodb_config

logger: Logger = getLogger(__name__)
logger.addHandler(NullHandler())


def main() -> None:
    """
    main program
    """
    try:
        mongo_db: MongoDB = MongoDB(make_mongodb_config("TEST_LOCAL", "test_collection1"))
        mongo_db.remove_all()

        documents: Sequence[Mapping[str, Any]] = [{
            "_id": 202107041000 + i,
            "data": {f"station_{j}": str(float(j)) for j in range(10)}
        } for i in range(5)]
        mongo_db.upsert_all(documents)

        documents2: Sequence[Mapping[str, Any]] = [{
            "_id": 202107041000 + i,
            "data": {f"station_{j}": str(float(j)+10.0) for j in range(7)}
        } for i in range(5)]
        mongo_db.upsert_each(documents2)

    except (OSError, Error) as e:
        logger.error(e.args)
        sys.exit(1)


if __name__ == '__main__':
    main()
