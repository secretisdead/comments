import uuid
import time
import re
from ipaddress import ip_address
from enum import Enum
from datetime import datetime, timezone

from sqlalchemy import Table, Column, LargeBinary
from sqlalchemy import Integer, String, MetaData, distinct
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, and_

from statement_helper import sort_statement, paginate_statement, id_filter
from statement_helper import time_cutoff_filter, string_like_filter
from statement_helper import remote_origin_filter
from idcollection import IDCollection
from parse_id import parse_id, get_id_bytes, generate_or_parse_id

class Comment:
	def __init__(
			self,
			id=None,
			creation_time=None,
			edit_time=0,
			subject_id='',
			remote_origin='127.0.0.1',
			user_id='',
			body='',
		):
		self.id, self.id_bytes = generate_or_parse_id(id)

		if None == creation_time:
			creation_time = time.time()
		self.creation_time = int(creation_time)
		self.creation_datetime = datetime.fromtimestamp(
			self.creation_time,
			timezone.utc,
		)

		self.edit_time = int(edit_time)
		self.edit_datetime = datetime.fromtimestamp(
			self.edit_time,
			timezone.utc,
		)

		self.subject_id, self.subject_id_bytes = parse_id(subject_id)
		self.subject = None

		self.remote_origin = ip_address(remote_origin)

		self.user_id, self.user_id_bytes = parse_id(user_id)
		self.user = None

		self.body = str(body)

class Comments:
	def __init__(self, engine, db_prefix='', install=False, remote_origin=None):
		self.engine = engine
		self.engine_session = sessionmaker(bind=self.engine)()

		self.db_prefix = db_prefix

		self.body_length = 128

		metadata = MetaData()

		default_bytes = 0b0 * 16

		# comments tables
		self.comments = Table(
			self.db_prefix + 'comments',
			metadata,
			Column(
				'id',
				LargeBinary(16),
				primary_key=True,
				default=default_bytes
			),
			Column('creation_time', Integer, default=0),
			Column('edit_time', Integer, default=0),
			Column(
				'subject_id',
				LargeBinary(16),
				default=default_bytes
			),
			Column(
				'remote_origin',
				LargeBinary(16),
				default=ip_address(default_bytes)
			),
			Column(
				'user_id',
				LargeBinary(16),
				default=default_bytes
			),
			Column('body', String(self.body_length)),
		)

		self.connection = self.engine.connect()

		if install:
			table_exists = self.engine.dialect.has_table(
				self.engine,
				self.db_prefix + 'comments'
			)
			if not table_exists:
				metadata.create_all(self.engine)

	def uninstall(self):
		for table in [
				self.comments,
			]:
			table.drop(self.engine)

	# retrieve comments
	def get_comment(self, id):
		comments = self.search_comments(filter={'ids': id})
		return comments.get(id)

	def prepare_comments_search_statement(self, filter):
		conditions = []
		conditions += id_filter(filter, 'ids', self.comments.c.id)
		conditions += time_cutoff_filter(
			filter,
			'created',
			self.comments.c.creation_time,
		)
		conditions += time_cutoff_filter(
			filter,
			'edited',
			self.comments.c.edit_time,
		)
		conditions += id_filter(
			filter,
			'subject_ids',
			self.comments.c.subject_id,
		)
		conditions += remote_origin_filter(
			filter,
			'remote_origins',
			self.comments.c.remote_origin,
		)
		conditions += id_filter(
			filter,
			'user_ids',
			self.comments.c.user_id,
		)
		conditions += string_like_filter(
			filter,
			'body',
			self.comments.c.body,
		)

		statement = self.comments.select()
		if conditions:
			statement = statement.where(and_(*conditions))
		return statement

	def count_comments(self, filter={}):
		statement = self.prepare_comments_search_statement(filter)
		statement = statement.with_only_columns(
			[func.count(self.comments.c.id)]
		)
		return self.connection.execute(statement).fetchone()[0]

	def search_comments(
			self,
			filter={},
			sort='',
			order='',
			page=0,
			perpage=None
		):
		statement = self.prepare_comments_search_statement(filter)

		statement = sort_statement(
			statement,
			self.comments,
			sort,
			order,
			'creation_time',
			True,
			[
				'creation_time',
				'id',
			],
		)
		statement = paginate_statement(statement, page, perpage)

		result = self.connection.execute(statement).fetchall()
		if 0 == len(result):
			return IDCollection()

		comments = IDCollection()
		for row in result:
			comment = Comment(
				id=row[self.comments.c.id],
				creation_time=row[self.comments.c.creation_time],
				edit_time=row[self.comments.c.edit_time],
				subject_id=row[self.comments.c.subject_id],
				remote_origin=row[self.comments.c.remote_origin],
				user_id=row[self.comments.c.user_id],
				body=row[self.comments.c.body],
			)

			comments.add(comment)
		return comments

	# manipulate comments
	def create_comment(self, **kwargs):
		comment = Comment(**kwargs)
		# preflight check for existing id
		if self.count_comments(filter={'ids': comment.id_bytes}):
			raise ValueError('Comment ID collision')
		self.connection.execute(
			self.comments.insert(),
			id=comment.id_bytes,
			creation_time=int(comment.creation_time),
			edit_time=int(comment.edit_time),
			subject_id=comment.subject_id_bytes,
			remote_origin=comment.remote_origin.packed,
			user_id=comment.user_id_bytes,
			body=str(comment.body),
		)
		return comment

	def update_comment(self, id, **kwargs):
		if 'edit_time' not in kwargs:
			kwargs['edit_time'] = time.time()
		comment = Comment(id=id, **kwargs)
		updates = {}
		if 'creation_time' in kwargs:
			updates['creation_time'] = int(comment.creation_time)
		updates['edit_time'] = int(comment.edit_time)
		if 'subject_id' in kwargs:
			updates['subject_id'] = comment.subject_id_bytes
		if 'remote_origin' in kwargs:
			updates['remote_origin'] = comment.remote_origin.packed
		if 'body' in kwargs:
			updates['body'] = str(comment.body)
		if 'user_id' in kwargs:
			updates['user_id'] = comment.user_id_bytes
		if 0 == len(updates):
			return
		self.connection.execute(
			self.comments.update().values(**updates).where(
				self.comments.c.id == comment.id_bytes
			)
		)

	def delete_comment(self, id):
		id = get_id_bytes(id)
		self.connection.execute(
			self.comments.delete().where(self.comments.c.id == id)
		)

	# anonymization
	def anonymize_id(self, id, new_id=None):
		id = get_id_bytes(id)

		if not new_id:
			new_id = uuid.uuid4().bytes

		self.connection.execute(
			self.comments.update().values(user_id=new_id).where(
				self.comments.c.user_id == id,
			)
		)

		return new_id

	def anonymize_comment_origins(self, comments):
		for comment in comments.values():
			if 4 == comment.remote_origin.version:
				# clear last 16 bits
				anonymized_origin = ip_address(
					int.from_bytes(comment.remote_origin.packed, 'big')
					&~ 0xffff
				)
			elif 6 == comment.remote_origin.version:
				# clear last 80 bits
				anonymized_origin = ip_address(
					int.from_bytes(comment.remote_origin.packed, 'big')
					&~ 0xffffffffffffffffffff
				)
			else:
				raise ValueError('Encountered unknown IP version')
			self.connection.execute(
				self.comments.update().values(
					remote_origin=anonymized_origin.packed
				).where(
					self.comments.c.id == comment.id_bytes
				)
			)
