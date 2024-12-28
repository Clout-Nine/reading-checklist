from deltalake import Field, Schema

from classes.data_table import DataTable


lotr_table: DataTable = DataTable(
  catalog_name='leisure',
  schema_name='literature',
  table_name='lord_of_the_rings',
  primary_keys=['book'],
  schema=Schema([
    Field('book', 'string'),
    Field('page_start', 'integer'),
    Field('page_end', 'integer'),
    Field('pages', 'integer')
  ])
)
