#!/usr/bin/env python
# -*- coding: utf-8 -*-

class _Engine(object):
	"""docstring for _Engine"""
	def __init__(self, connect):
		self._connect = connect

	def connect(self):
		return self._connect

engine = None

class _DbCtx(threading.local):

	def __init__(self):
		self.connect      = None
		self.transactions = 0


	def is_init(self):
		return not self.connect is None

	def init(self):
		self.connect      = _LasyConnection()
		self.transactions = 0	

	def cleanup(self):
		self.connect.cleanup()
		self.connect = None

	def cursor(self):
		return self.connect.cursor()

# _db_ctx = _DbCtx()			

class _ConnectCtx(object):

	def __enter__(self):
		global _db_ctx 
		self.should_cleanup = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True
		return self

	def __exit__(self):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

	def connection():
		return _ConnectCtx()

class _Transactions(object):
	"""docstring for _Transactions"""
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init:
				_db_ctx.init()
				self.should_close_conn = True
				_db_ctx.transactions = _db_ctx.transactions+1
		return self

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions -1

		try:
			if _db_ctx.transactions==0:
				if exctype is None:
					self.commit()
				else
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
			global _db_ctx
			try:
				_db_ctx.connect.commit()

			except:
				_db_ctx.connect.rollback()
				raise
	def rollback(self):
		global _db_ctx
		_db_ctx.connect.rollback()

