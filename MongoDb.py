"""
MongoDB/AWS DocumentDB登録のための観測所マスター情報
"""
from __future__ import annotations

import copy
import dataclasses
import inspect
import urllib.parse
from datetime import datetime, timezone
from logging import Logger, getLogger, NullHandler
from pathlib import Path
from typing import Optional, Any, Sequence, Mapping, MutableMapping, Union

from pymongo import MongoClient
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

from Fundamental import InsertError, DBError, JST, DataReadError

logger: Logger = getLogger(__name__)
logger.addHandler(NullHandler())


@dataclasses.dataclass(frozen=True)
class MongoDBConfig:
    """
    MongoDB接続設定
    """
    host: str  # ホスト
    user_name: str  # ユーザ名
    password: str  # パスワード
    database: str  # データベース名
    collection: str  # コレクション名
    ca_file: Optional[Path] = None  # CAファイルパス
    replica_set: Optional[str] = None  # レプリカセット
    read_preference: Optional[str] = None  # 読み取り負荷分散オプション
    port: int = 27017  # ポート

    def __post_init__(self):
        if self.ca_file is not None:
            if not self.ca_file.is_file():
                raise DataReadError(f"The specified SSL CA file {str(self.ca_file)} is not found."
                                    f" Please bring it there."
                                    f" ({inspect.currentframe().f_code.co_name} in module {__name__}).")

    @property
    def uri(self) -> str:
        """
        MongoDB URI
        Returns:
            URI(str)
        """
        username: str = urllib.parse.quote_plus(self.user_name)
        password: str = urllib.parse.quote_plus(self.password)
        return f'mongodb://{username}:{password}@{self.host}:{self.port}/{self.database}'

    @property
    def pymongo_option_dict(self) -> Mapping[str, Any]:
        """
        Pymongoのオプション辞書
        Returns:
            オプション辞書(Mapping[str, Any])
        """
        option_dict: MutableMapping[str, Any] = dict()
        if self.ca_file is not None:
            option_dict["ssl"] = True
            option_dict["ssl_ca_certs"] = str(self.ca_file.resolve())
        if self.replica_set is not None:
            option_dict["replicaset"] = self.replica_set
        if self.read_preference is not None:
            option_dict["read_preference"] = self.read_preference
        return option_dict


def make_mongodb_config(
        database_name: str,
        collection_name: str
) -> MongoDBConfig:
    """
    MongoDB/DocumentDB設定
    Args:
        database_name(str): データベース名
        collection_name(str): コレクション名
    Returns:
        DB設定(MongoDBConfig)
    """
    try:
        return MongoDBConfig(
            host="localhost",
            user_name="test",
            password="Apollo13",
            database=database_name,
            collection=collection_name)
    except KeyError as e:
        raise DataReadError(f"The database setting does not have enough items."
                            f" Please check the 'database' dictionary in a setting file."
                            f" ({inspect.currentframe().f_code.co_name} in module {__name__},"
                            f" message: {e.args}).")


class MongoDB:
    """
    MongoDB/DocumentDBアクセスのラッパクラス

    Attributes:
        __client (pymongo.MongoClient): MongoDBのデータベース
        __collection (pymongo.Collection): MongoDBのデータベース内コレクション
    """
    def __init__(self, config: MongoDBConfig):
        """
        設定辞書データ(たいていconfig.json)のDB設定を読み込み、DBへの接続準備をする。

        Args:
            config: 辞書データ。
                    "ca_file", "host", "port", "user_name", "password", "database","collection"
                    のフィールドを持つ。
        """
        try:
            with MongoClient(config.uri, **config.pymongo_option_dict) as self.__client:
                self.__collection = self.__client[config.database].get_collection(config.collection)
        except ServerSelectionTimeoutError as e:
            raise DBError(e.args)

    @property
    def all_documents(self) -> Sequence[Mapping[str, Any]]:
        """
        コレクションのすべてのドキュメントのリストを返す。

        Returns:
             ドキュメントのリスト。

        """
        try:
            return list(self.__collection.find())
        except OperationFailure as e:
            raise DBError(e.args)

    @property
    def first_document(self) -> Optional[Any]:
        """
        コレクションの最初のドキュメントのゲッタ

        Returns:
            コレクションの最初のドキュメント(Optional[Mapping[str, Any]])
        """
        try:
            found: Sequence[Any] = list(self.__collection.find_one())
            if len(found) > 0:
                return next(iter(found))
            else:
                return None
        except OperationFailure as e:
            raise DBError(e.args)

    def insert(self, document: Mapping[str, Any]) -> None:
        """
        コレクションに新たなドキュメントをinsertする。

        Args:
            document(Mapping[str, Any]): 書き込みたいドキュメント
        """
        try:
            doc_id = identity(document)
            if len(self.select(doc_id)) == 0:
                result = self.__collection.insert_one(document)
            else:
                result = self.__collection.replace_one(doc_id, document)
            if not result.acknowledged:
                raise InsertError(f"write failed for {result.inserted_id}")
        except OperationFailure as e:
            raise DBError(e.args)

    def insert_all(self, documents: Sequence[Mapping[str, Any]]) -> None:
        """
        コレクションに新たなドキュメント列を全てinsert
        Args:
            documents(Sequence[Mapping[str, Any]]):  書き込みたいドキュメント列
        """
        try:
            documents_with_create_time: Sequence[MutableMapping[str, Any]] = [
                dict(copy.deepcopy(document)) for document in documents]
            for document in documents_with_create_time:
                document["createTime"] = datetime.now(tz=JST)
            self.__collection.insert_many(documents_with_create_time)
        except OperationFailure as e:
            raise DBError(e.args)

    def upsert_stations(self, documents: Sequence[Mapping[str, Any]]) -> None:
        """
        コレクションに観測所ごとのデータドキュメント列をupsertで登録
        Args:
            documents(Sequence[Mapping[str, Any]]):  書き込みたいドキュメント列
        """
        try:
            for document in documents:
                for station_id, station_data in document["data"].items():
                    self.__collection.update_one(
                        {r"_id": document["_id"]},
                        {r"$set": {f"data.{station_id}": station_data},
                         r"$setOnInsert": {"createTime": datetime.now(timezone.utc)}},
                        upsert=True)
        except OperationFailure as e:
            raise DBError(e.args)

    def upsert_all(self, documents: Sequence[Mapping[str, Any]]) -> None:
        """
        upsert
        Args:
            documents(Sequence[Mapping[str, Any]]):  書き込みたいドキュメント列
        """
        try:
            for document in documents:
                self.__collection.update_one(
                    {r"_id": document["_id"]},
                    {r"$set": document,
                     r"$setOnInsert": {"createTime": datetime.now(timezone.utc)}},
                    upsert=True)
        except OperationFailure as e:
            raise DBError(e.args)

    def upsert_each(self, documents: Sequence[Mapping[str, Any]]) -> None:
        """
        コレクションに観測所ごとのデータドキュメント列をupsertで登録
        Args:
            documents(Sequence[Mapping[str, Any]]):  書き込みたいドキュメント列
        """
        try:
            for document in documents:
                self.__collection.update_one(
                    {r"_id": document["_id"]},
                    {r"$set": {f"data": document["data"]},
                     r"$setOnInsert": {"createTime": datetime.now(timezone.utc)}},
                    upsert=True)
        except OperationFailure as e:
            raise DBError(e.args)

    def remove_all(self):
        """
        コレクションのドキュメントを全て削除
        """
        try:
            self.__collection.remove()
        except OperationFailure as e:
            raise DBError(e.args)

    def replace_all(self, documents: Sequence[Mapping[str, Any]]) -> None:
        """
        コレクションのドキュメントを全て入れ替え（前のものを全て削除してから全て挿入）
        Args:
            documents(Sequence[Mapping[str, Any]]):  書き込みたいドキュメント列
        """
        self.remove_all()
        self.insert_all(documents)

    def update(self, update_field: Mapping[str, Any]) -> None:
        """
        コレクションのフィールドを置き換える。
        Args:
            update_field(Mapping[str, Any]): 置き換えるべきフィールド
        """
        try:
            self.__collection.update_one(update_field["_id"], {'$set': update_field})
        except OperationFailure as e:
            raise DBError(e.args)

    def delete(self, field_key: Mapping[str, Any]) -> None:
        """
        コレクションの、指定された"_id"キーを持つフィールドを削除する。
        Args:
            field_key(Mapping[str, Any]): 削除すべきフィールドの"_id"キー
        """
        try:
            self.__collection.delete_one(field_key)
        except OperationFailure as e:
            raise DBError(e.args)

    def select(self, field: Mapping[str, Any]) -> Sequence[Any]:
        """
        コレクションの、指定されたフィールドキーを持つドキュメントのリストを取得する。

        Args:
            field(Mapping[str, Any]): 取得すべきフィールドのキー

        Returns:
            取得したドキュメントのリスト
        """
        try:
            return list(self.__collection.find(field))
        except OperationFailure as e:
            raise DBError(e.args)


def identity(input_document: Mapping[str, Any]) -> Union[Mapping[str, Any], Any]:
    """
    ドキュメントの"_id"フィールドの辞書を返す

    Args:
        input_document(Mapping[str, Any]): ドキュメントの辞書データ

    Returns:
        "_id"フィールドの辞書(Mapping[str, Any])
    """
    return {"_id": input_document["_id"]}
