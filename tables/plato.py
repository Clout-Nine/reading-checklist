from deltalake import Field, Schema

from classes.data_table import DataTable


plato_table: DataTable = DataTable(
  catalog_name='dissertation',
  schema_name='plato',
  table_name='dialogue_pages',
  primary_keys=['book'],
  schema=Schema([
    Field('book', 'string'),
    Field('page_start', 'integer'),
    Field('page_end', 'integer'),
    Field('pages', 'integer')
  ])
)