# encoding: utf-8
"""
db数据库操作模块。默认pymysql+sqlalchemy
@author Yuriseus
@create 2016-8-4 14:03
"""
from math import ceil

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta, declared_attr
from sqlalchemy.orm import scoped_session, sessionmaker, Query
from sqlalchemy.sql import text

from config import settings


class ModelMeta(DeclarativeMeta):
    def __new__(cls, name, bases, d):
        column_name_sets = set()
        if 'column_name_sets' in d:
            for name in d['column_name_sets']:
                column_name_sets.add(name)
        else:
            for k, v in d.items():
                if getattr(v, '__class__', None) is None:
                    continue
                if v.__class__.__name__ == 'Column':
                    column_name_sets.add(k)

        # obj = type.__new__(cls, name, bases, dict(namespace))
        instance = super(ModelMeta, cls).__new__(cls, name, bases, dict(d))
        instance._column_name_sets = column_name_sets
        return instance

__Base = declarative_base(metaclass=ModelMeta)


class BaseModel(__Base):
    __abstract__ = True

    # 基类的 _column_name_sets  是为实现的类型
    _column_name_sets = NotImplemented

    _mapper = {}

    def to_dict(self):
        return dict(
            (column_name, getattr(self, column_name, None)) for column_name in self._column_name_sets
        )

    @classmethod
    def get_column_name_sets(cls):
        """
        获取 column 的定义的名称(不一定和数据库字段一样)
        """
        return cls._column_name_sets

    @declared_attr
    def Q(cls):
        return SQLAlchemy.instance().query()

    @declared_attr
    def session(cls):
        return SQLAlchemy.instance().session

    @declared_attr
    def connect(cls):
        return SQLAlchemy.instance().connect

    @classmethod
    def execute_query(cls, sql, param_dict):
        return SQLAlchemy.instance().execute_query(sql, param_dict)

    @classmethod
    def execute_update(cls, sql, param_dict):
        return SQLAlchemy.instance().execute_update(sql, param_dict)

    @classmethod
    def get_table_name(cls, column_value):
        return cls.__tablename__

    @classmethod
    def gen_model(cls, column_value_dict=None):
        table_name = cls.get_table_name(column_value_dict)
        spa = table_name.split('_')
        index = spa[len(spa) - 1]
        class_name = cls.__name__ + index
        model_dict = {
            '__module__': __name__,
            '__name__': class_name,
            '__tablename__': table_name,
            'column_name_sets': cls._column_name_sets    # 传入列名，否则Model的to_dict因找不到列名而找不到对应属性
        }

        model_class = BaseModel._mapper.get(class_name, None)
        if model_class is None:
            model_class = type(class_name, (BaseModel,), model_dict)
            BaseModel._mapper[class_name] = model_class

        model = model_class()
        if column_value_dict:    # 列值，新增时用到
            for k, v in column_value_dict.items():
                setattr(model, k, v)
        return model

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return str(self.to_dict())


class Pagination(object):
    """分页对象"""

    def __init__(self, query, current_page, page_size, total, items):
        # 查询对象
        self.query = query
        # 当前页，从1开始
        self.current_page = current_page
        # 每页显示数量
        self.page_size = page_size
        # 总记录数
        self.total = total
        # 当前页数据对象
        self.items = items

    @property
    def pages(self):
        if self.page_size == 0:
            pages = 0
        else:
            pages = int(ceil(self.total / float(self.page_size)))
        return pages

    def prev(self, error_out=False):
        return self.query.paginate(self.current_page - 1, self.page_size, error_out)

    @property
    def prev_num(self):
        return self.current_page - 1

    @property
    def has_prev(self):
        return self.current_page > 1

    def next(self, error_out=False):
        return self.query.paginate(self.current_page + 1, self.page_size, error_out)

    @property
    def has_next(self):
        return self.current_page < self.pages

    @property
    def next_num(self):
        return self.current_page + 1

    def to_dict(self):
        is_need_transform = False
        if self.items and isinstance(self.items[0], BaseModel):
            is_need_transform = True
        if is_need_transform:
            items = [item.to_dict() for item in self.items]
        else:
            items = self.items
        return {
            'current_page': self.current_page,
            'page_size': self.page_size,
            'total': self.total,
            'items': items
        }


class BaseQuery(Query):

    def __init__(self, entities, session=None):
        super().__init__(entities, session)
        self.current_page = 1
        self.page_size = 10

    def paginate(self, current_page, page_size=10, default=None):
        """
        执行分页查询
        :param current_page:
        :param page_size:
        :param default:
        :return: Pagination
        """
        self.current_page = current_page
        self.page_size = page_size
        if current_page < 1:
            return default
        items = self.limit(page_size).offset((current_page - 1) * page_size).all()
        if not items and current_page != 1:
            return default

        if current_page == 1 and len(items) < page_size:
            total = len(items)
        else:
            total = self.order_by(None).count()

        return Pagination(self, current_page, page_size, total, items)

    def get_items(self):
        return self.limit(self.page_size).offset((self.current_page - 1) * self.page_size).all()

    def get_count(self):
        return self.order_by(None).count()


class SqlQuery(object):
    """
    sql查询对象，根据传入的sql语句查询。
    提供分页查询
    """
    def __init__(self, sql, param_dict=None, sqlalchemy=None):
        self.sql = sql
        self.param_dict = param_dict
        self.current_page = 1
        self.page_size = 10
        if not sqlalchemy:
            sqlalchemy = SQLAlchemy.instance()
        self.sqlalchemy = sqlalchemy

    def paginate(self, current_page, page_size=10):
        """
        执行分页查询
        :param current_page:
        :param page_size:
        :return: Pagination
        """
        self.current_page = current_page
        self.page_size = page_size
        if current_page < 1:
            return []
        items = self.get_items()
        if not items and current_page != 1:
            return []

        if current_page == 1 and len(items) < page_size:
            total = len(items)
        else:
            total = self.get_count()

        return Pagination(self, current_page, page_size, total, items)

    def get_items(self):
        sql = self.sql + ' LIMIT %s, %s' % ((self.current_page - 1) * self.page_size, self.page_size)
        return self.sqlalchemy.execute_query(sql, self.param_dict)

    def get_count(self):
        # 替换查询字段为count(*)
        index = self.sql.find('FROM')
        if index == -1:
            index = self.sql.find('from')
        sql = 'select count(*) as total ' + self.sql[index:]
        result = self.sqlalchemy.execute_query(sql, self.param_dict)
        return result[0]['total']

    def all(self):
        return self.sqlalchemy.execute_query(self.sql, self.param_dict)

    def first(self):
        result = self.sqlalchemy.execute_query(self.sql, self.param_dict)
        if result:
            return result[0]
        else:
            return None


class SQLAlchemy(object):

    def __init__(self, host, port, user, password, db, **kwargs):
        param = {'host': host, 'port': port, 'user': user, 'password': password, 'db': db}
        self._db_url = 'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'.format(**param)
        self._session = None
        self._connect = None

    @staticmethod
    def instance():
        if not hasattr(SQLAlchemy, "_instance"):
            conf = settings.CONF['db']
            SQLAlchemy._instance = SQLAlchemy(conf['host'], conf['port'], conf['user'], conf['password'], conf['db'])
        return SQLAlchemy._instance

    @property
    def session(self):
        if not self._session:
            self._session = self.create_session(self._db_url)
        return self._session

    @property
    def connect(self):
        if not self._connect:
            engine = create_engine(self._db_url, echo=False)
            self._connect = engine.connect()
        return self._connect

    def query(self):
        # BaseQuery 目前提供了分页查询
        return self.session.query_property(BaseQuery)    # 查询出模型属性值

    @staticmethod
    def sql_query(sql, param_dict=None, sqlalchemy=None):
        return SqlQuery(sql, param_dict, sqlalchemy)

    def execute_query(self, sql, param_dict):
        stmt = text(sql)
        if param_dict and isinstance(param_dict, dict):
            stmt = stmt.bindparams(**param_dict)
        records = []
        row_proxys = self.connect.execute(stmt).fetchall()
        for _, row in enumerate(row_proxys):
            one = {}
            for i, t in enumerate(row.items()):
                one[t[0]] = t[1]
            records.append(one)
        return records

    def execute_update(self, sql, param_dict):
        stmt = text(sql)
        if param_dict and isinstance(param_dict, dict):
            stmt = stmt.bindparams(**param_dict)
        result_proxy = self.connect.execute(stmt)
        return result_proxy.lastrowid

    @staticmethod
    def create_session(db_url, **kwargs):
        engine = create_engine(db_url, echo=True)
        session = sessionmaker(autocommit=False, **kwargs)    # 默认关闭事务
        session.configure(bind=engine)
        return scoped_session(session)

    def remove(self):
        if self._session:
            self._session.remove()


if __name__ == '__main__':
    pass
    # config = settings.CONF['db']
    # from models.user import User
    # sqlalchemy = SQLAlchemy('localhost', 3306, 'root', 'root', 'test')
    # user = User.Q.first()
    # user.name = 'Hello Yuri'
    # User.session.add(user)
    # User.session.commit()
    # from models.user import User
    # l = User.session.query(User).all()
    # sql = 'select * from user where name= :name'
    # l = User.execute_query(sql, {'name': 'yuri'})
    # sql = 'INSERT INTO user (name, age) VALUES (:name, :age)'
    # l = User.execute_update(sql, {'name': 'hh', 'age': 22})

