#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Simona Zhu'
#Web App里面有很多地方都要访问数据库。访问数据库需要创建数据库连接、游标对象，
# 然后执行SQL语句，最后处理异常，清理资源。这些访问数据库的代码如果分散到各个函数中，
# 势必无法维护，也不利于代码复用。所以，我们要首先把常用的SELECT、INSERT、UPDATE和DELETE操作用函数封装起来。


import asyncio, logging

import aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql)
    #Logs a message with level INFO on this logger.以INFO标准记录消息日志
    #这就是logging的好处，它允许你指定记录信息的级别，有debug，info，warning，error等几个级别，
    # 当我们指定level=INFO时，logging.debug就不起作用了。同理，指定level=WARNING后，debug和info就不起作用了。
    # 这样一来，你可以放心地输出不同级别的信息，也不用删除，最后统一控制输出哪个级别的信息。

#我们需要创建一个全局的连接池，每个HTTP请求都可以从连接池中直接获取数据库连接。
#使用连接池的好处是不必频繁地打开和关闭数据库连接，而是能复用就尽量复用。
#连接池由全局变量__pool存储，缺省情况下将编码设置为utf8，自动提交事务


async def create_pool(loop,**kw):
    logging.info('create database connection pool...')
    global __pool
    _pool=await aiomysql.create_pool(
        #create_pool(minsize=10, maxsize=10, loop=None, **kwargs)
        #A coroutine that creates a pool of connections to MySQL database.
        #创建连接池的协程
        #get函数第二个参数是缺省值，返回键不存在的情况下默认值
        host=kw.get('host','localhost'),
        port=kw.get('port',3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset','utf8'),
        autocomit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop
    )
#要执行SQL的SELECT语句，我们用select函数执行，需要传入SQL语句和SQL参数
async def select(sql,args,size=None):
    log(sql,args)#打印日志消息
    global  __pool
    with(await __pool) as conn:
        #当python执行这一句时，会调用__enter__函数，然后把该函数return的值传给as后指定的变量。之后，
        # python会执行下面do something的语句块。最后不论在该语句块出现了什么异常，都会在离开时执行__exit__。
        #也就是说用这种写法，conn会自动关闭，不必手动关闭即不必频繁地打开连接和关闭连接，能复用就复用
        cur=await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?','%s'),args or ())#这是数据库的execute函数，和下面定义的无关
        if size:
            rs=await cur.fetchmany(size)
        else:
            rs=await cur.fetchall()
            await cur.close()
            logging.info('rows returned:%s'%len(rs))
            return rs
#SQL语句的占位符是?，而MySQL的占位符是%s，#execute语句里应该是应用于mysql的语句，因此将占位符换成%s
#select()函数在内部自动替换。注意要始终坚持使用带参数的SQL，而不是自己拼接SQL字符串，
#这样可以防止SQL注入攻击。如果传入size参数，就通过fetchmany()获取最多指定数量的记录
#否则，通过fetchall()获取所有记录。

#要执行INSERT、UPDATE、DELETE语句，可以定义一个通用的execute()函数，
# 因为这3种SQL的执行都需要相同的参数，以及返回一个整数表示影响的行数：
async def execute(sql,args):
    log(sql)
    with(await __pool) as conn:
        try:
            cur=await conn.cursor()
            await cur.execute(sql.replace('?','%s'),args)
            affected=cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected

def create_args_string(num):
    L = []
    for n in range(num):
         L.append('?')
    return ', '.join(L)
#Model从dict继承，所以具备所有dict的功能，同时又实现了特殊方法__getattr__()和__setattr__()，
class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)
#execute()函数和select()函数所不同的是，cursor对象不返回结果集，而是通过rowcount返回结果数。
#在类级别上定义的属性用来描述User对象和表的映射关系，类的每一个属性在表格中就是一列
#而实例属性必须通过__init__()方法去初始化，所以两者互不干扰
class ModelMetaclass(type):
    def __new__(cls, name,bases,attrs):
        #排除Model类本身：
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        #获取table名称：
        tableName=attrs.get('__table__',None) or name
        logging.info('found model:%s(table:%s)' %(name,tableName))
        #获取所有的Field和主键名：
        mappings=dict()
        fields=[]
        primaryKey=None
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                # v是属于Field类的，因此v遇到打印等输出时会自动调用__str__
                # attr是无序的字典，因此打印输出的时候也是无序的
                mappings[k]=v
                #在当前类（比如User）中查找定义的类的所有属性，

                if v.primary_key:
                    #找到主键
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field:%s' %k)
                        #主键只能有一个，确定以后再设就会报错
                    primaryKey=k
                else:
                    fields.append(k)

            if not primaryKey:
                raise RuntimeError('Primary key not found.')
            for k in mappings.keys():
                attrs.pop(k)
                # 从类属性中删除该Field属性，否则，容易造成运行时错误（实例的属性会遮盖类的同名属性）；
            escaped_fields=list(map(lambda f: '`%s`' % f, fields))#fields存放的是列名，将列名转化成字符串格式
            #增加属性内容
            attrs['__mappings__']=mappings#保存属性和列的映射关系
            attrs['__table__'] = tableName
            attrs['__primary_key__'] = primaryKey  # 主键属性名
            attrs['__fields__'] = fields  # 除主键外的属性名
            # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
            attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
            attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
            tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
            attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
            tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
            attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
            return type.__new__(cls, name, bases, attrs)
        #table名和主键名都要用`%s`

class Model(dict, metaclass=ModelMetaclass):
    def __init__(self,**kw):
        super(Model,self).__init__(**kw)#super从父类继承
    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
    def __setattr__(self,key,value):
        self[key]=value
    def getValue(self,key):
        return getattr(self,key,None)#None是缺省值
    def getValueOrDefault(self,key):
        value=getattr(self,key,None)
        if value is None:
            field=self.__mappings__[key]
            if field.default is not None:#每一列的内容都是有default值得
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    #通过classmethod装饰器，可以是本类调用也可以是本类的实例调用，实例调用时类调用被忽略
    # 实例的属性会遮盖类的同名属性），也可以让子类调用方法，并且子类默认作为第一个参数
    #每一个函数每一个IO操作都要用异步
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]#取出select语句，此时sql是一个list
        if where:#如果有where限定则加上
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        #LIMIT 接受一个或两个数字参数。参数必须是一个整数常量。如果给定两个参数，
        # 第一个参数指定第一个返回记录行的偏移量，第二个参数指定返回记录行的最大数目。
        # 初始记录行的偏移量是 0(而不是 1)： 为了检索从某一个偏移量到记录集的结束所有的记录行，
        # 可以指定第二个参数为 - 1：如果只给定一个参数，它表示返回最大的记录行数目：
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)#extend向list 追加,追加的内容可以是元组也可以是list也可以是set

            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        #不懂_num_
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)#select函数的参数size，size表示限定一次读取几行
        if len(rs) == 0:
            return None
        return rs[0]['_num_']#'_num_'是什么

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))#函数作用到每一个并且将结果返回
        args.append(self.getValueOrDefault(self.__primary_key__))#主键和其他列名分开操作
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    @classmethod
    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    @classmethod
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)#按照主键删除
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)


