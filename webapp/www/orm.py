'''
ORM 使用mysql
-----------------------------------
封装常用的select, insert, update和delete操作
既然框架使用了异步的aiohttp,那么所有操作都要使用异步(原则问题)
所以使用mysql的异步驱动aiomysql
'''
import logging
import aiomysql


async def create_pool(loop, **kw):
    '''
    创建连接池
    ------------------------------
    让每一个HTTP请求都可以从连接池中直接获取数据库连接.
    使用连接池的好处是不必频繁的打开和关闭数据库连接,而是能进行复用
    连接池由全局变量__pool存储
    '''
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

async def select(sql, args, size=None):
    '''
    SELECT语句
    -----------------------------------
    通过传入sql语句和参数来进行数据库的操作.size参数可选返回指定数量的结果
    需要注意的是,sql语句的占位符是'?',而mysql的是'%s',所以要进行一次替换
    (始终坚持使用带参数的sql,而不是自己拼接sql字符串,防止注入攻击)
    '''
    logging.info('SQL: %s' % sql)
    global __pool
    async with __pool as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql, args):
    '''
    insert, update, delete语句的通用函数
    -----------------------------------
    这三个操作参数相同,最后返回一个整数表示影响的行数
    '''
    logging.info('SQL: %s' % sql)
    global __pool
    async with __pool as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected

def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ','.join(L)

class ModelMetaclass(type):
    '''
    Model类的元类
    --------------------------------
    所有元类都继承自type

    '''
    def __new__(cls, name, bases, attrs):
        '''
        __new__方法在__init__之前执行,用于控制类的初始化
        name: 要__init__的类
        bases: 继承父类的集合
        attrs: 类的方法集合
        '''
        # 排除对Model本身的修改
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table的名称(保存在__table__里)
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s(table: %s)' % (name, tableName))
        # 这里的mappings是一个字典,用于保存所有Field
        mappings = dict()
        fields = []  # 保存field
        primaryKey = None  # 用于保存主键
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到为主键的Field
                    if primaryKey:
                        # 这里表示有多个主键了
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    # 得到主键的字段名
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            # 若没有主键
            raise RuntimeError('Primary key not found.')
        # 把所有field从类方法中去掉
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        # 保存属性与列的映射关系
        attrs['__mappings__'] = mappings
        # 保存table的名称
        attrs['__table__'] = tableName
        # 保存主键的名称
        attrs['__primary_key__'] = primaryKey
        # 保存除主键外的field集
        attrs['__fields__'] = fields
        # 构造默认的数据库操作sql语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields)+1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ','.join(map(lambda f:'`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):
    '''
    所有orm映射的基类
    --------------------------------
    继承自dict,所以具备dict的功能,同时又实现了两个特殊方法:
    __getattr__和__setattr__, 可以用类似: user[id]与user.id这样来写
    '''
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # 以下为具体的类方法
    @classmethod
    async def find(cls, pk):
        '''通过主键查找数据'''
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        '''保存数据'''
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failded to insert record: affected rows: %s' % rows)

class Field(object):
    '''
    各种field的基类
    '''
    def __init__(self, name, column_type, primary_key, default):
        self.name = name  # 字段名
        self.column_type = column_type  # 字段数据类型
        self.primary_key = primary_key  # 是否为主键
        self.default = default  # 有无默认值

    def __str__(self):
        return '<%s, %s, %s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    '''
    用于映射varchar的StringField
    '''
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super(StringField, self).__init__(name, ddl, primary_key, default)

