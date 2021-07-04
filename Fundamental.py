"""
Fundamentalモジュール: 基本ユーティリティ
"""
from __future__ import annotations

import os
from datetime import timezone, timedelta
from itertools import chain
from logging import Logger, getLogger, NullHandler, FileHandler, DEBUG, INFO, StreamHandler, WARNING, Formatter, \
    basicConfig
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Mapping, Final, Iterable, Sequence, Generator, TypeVar, Any

logger: Logger = getLogger(__name__)
logger.addHandler(NullHandler())

T = TypeVar("T")
JST: timezone = timezone(timedelta(hours=+9), 'JST')


class Error(Exception):
    """
    パッケージ内例外の基本クラス
    """
    pass


class DataWriteError(Error):
    """
    データ読み出し失敗の例外
    """
    pass


class DataReadError(Error):
    """
    データ書き込み失敗の例外
    """
    pass


class ImageAnalysisError(Error):
    """
    画像解析失敗の例外
    """
    pass


class NoNewDataException(Error):
    """
    新規データがない例外
    """
    pass


class InsertError(Error):
    """
    DB書き込みエラー
    """
    pass


class DBError(Error):
    """
    DBオペレーションでのエラー
    """
    pass


class UsageError(Error):
    """
    関数の使用法が間違っているバグのエラー
    """
    pass


def prepare_output_directory(
        output_directory: Path,
        warning=True
) -> None:
    """
    ディレクトリ生成
    Args:
        output_directory(pathlib.Path): ディレクトリパス
        warning(bool): 生成時にメッセージを表示するかどうか
    """
    if not output_directory.is_dir():
        if warning:
            logger.info(f"The specified output directory {output_directory} does not exist."
                        f"Making the directory.")
        try:
            output_directory.mkdir(parents=True, exist_ok=True)
        except FileExistsError as e:
            raise DataWriteError(e.args)


def adjust_data_value(data_value: str, disabled_value_str=None) -> str:
    """
    数値文字列が数値に変換可能ならそのまま。そうでなければ無効地の文字列("-999.9")。
    Args:
        data_value(str): 文字列
        disabled_value_str(str, optional): 無効値の文字列
    Returns:
        数値文字列(str)
    """
    if not is_convertible_to_float(data_value):
        return disabled_value_str if disabled_value_str is not None else default_disabled_value_str()
    return data_value


def default_disabled_value_str() -> str:
    """
    デフォルトの無効値文字列
    Returns:
        "-999.9" (str)
    """
    return "-999.9"


def is_convertible_to_float(string: str) -> bool:
    """
    文字列がfloatに変換可能か
    Args:
        string(str): 文字列

    Returns:
        変換可能ならTrue
    """
    # return string.strip().replace(',', '').replace('.', '').replace('-', '').isnumeric()
    try:
        float(string)
    except ValueError:
        return False
    return True


def set_logging(log_file: Path, max_bytes=100000, backup_count=5) -> None:
    """
    ロギングの基本設定
    Args:
        log_file(pathlib.Path): ログファイルのパス
        max_bytes(int): ログ回転時の1ファイルの最大容量
        backup_count(int): ログ回転時のファイル保存の最大数
    """
    print(f"log is being written in {log_file.resolve()}")
    prepare_output_directory(log_file.parent)
    formatting: Final[str] = "%(asctime)s %(levelname)-8s [%(module)s#%(funcName)s %(lineno)d] %(message)s"

    file_handler: FileHandler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    if os.getenv("DEBUG") == "1":
        file_handler.setLevel(DEBUG)
    else:
        file_handler.setLevel(INFO)
    file_handler.setFormatter(Formatter(formatting))

    stream_handler: StreamHandler = StreamHandler()
    stream_handler.setLevel(WARNING)
    stream_handler.setFormatter(Formatter(formatting))

    # noinspection PyArgumentList
    basicConfig(
        level=DEBUG,
        handlers=[file_handler, stream_handler])


def merge_mappings(dictionaries: Iterable[Mapping]) -> Mapping:
    """
    辞書のマージ
    Args:
        dictionaries(Iterable[Mapping]):

    Returns:
        マージした辞書(Mapping)
    """
    return {key: value for dictionary in dictionaries for key, value in dictionary.items()}


def split_sequence(sequence: Sequence, sublist_size: int) -> Generator[Sequence, Any, None]:
    """
    Sequenceをsub-sequencesに分割する
    Args:
        sequence(Sequence): リスト
        sublist_size(int): サブリストの要素数
    Returns:
        サブリストのGenerator
    """
    for index in range(0, len(sequence), sublist_size):
        yield sequence[index:index + sublist_size]


def split_sequence_eager(sequence: Sequence, sublist_size: int) -> Sequence[Sequence]:
    """
    Sequenceをsub-sequencesに分割し、内部は評価したリスト
    Args:
        sequence(Sequence): リスト
        sublist_size(int): サブリストの要素数
    Returns:
        サブリストのリスト(Sequence[Sequence])
    """
    return list(split_sequence(sequence, sublist_size))


def transpose(matrix: Iterable[Iterable[T]]) -> Iterable[Iterable[T]]:
    """
    行列の転置。ジェネレータなどの場合でも評価はされない。
    Args:
        matrix(Iterable[Iterable[T]]): 行列

    Returns:
        転置行列(Iterable[Iterable[T]])
    """
    return zip(*matrix)


def transpose_eager(matrix: Iterable[Iterable[T]]) -> Sequence[Sequence[T]]:
    """
    行列の転置。要素を評価してリストのリストにする。
    Args:
        matrix(Iterable[Iterable[T]]): 行列

    Returns:
        転置行列(Sequence[Sequence[T]])
    """
    return list(list(column for column in row) for row in transpose(matrix))


def flatten(matrix: Iterable[Iterable[T]]) -> Iterable[T]:
    """
    行列の平坦化
    e.g. [[0,1,2,3],[4,5,6,7]] -> [0,1,2,3,4,5,6,7]
    ジェネレータなどの場合、評価はされない。
    Args:
        matrix(Iterable[Iterable[T]]): 行列

    Returns:
        平坦化されたIterable(Iterable[T])
    """
    return chain(*matrix)


def flatten_eager(matrix: Iterable[Iterable[T]]) -> Sequence[T]:
    """
    行列の平坦化
    e.g. [[0,1,2,3],[4,5,6,7]] -> [0,1,2,3,4,5,6,7]
    要素は評価する。
    Args:
        matrix(Iterable[Iterable[T]]): 行列

    Returns:
        要素を評価して平坦化されたSequence(Sequence[T])
    """
    return list(chain(*[list(column for column in row) for row in matrix]))
