from deltalake import Field, Schema

from classes.data_table import DataTable


bible_table: DataTable = DataTable(
  catalog_name='devotions',
  schema_name='scripture',
  table_name='bible',
  primary_keys=['book'],
  schema=Schema([
    Field('order', 'integer'),
    Field('book', 'string'),
    Field('page_start', 'integer'),
    Field('page_end', 'integer'),
    Field('pages', 'integer')
  ])
)