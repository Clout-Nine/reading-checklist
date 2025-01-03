import polars as pl

from deltalake import Field, Schema
from jinja2 import Template
from math import ceil, floor
from typing import Any

from classes.data_table import DataTable


class ReadingPlanner:
  """Class to generate a reading plan based on inputted books"""
  def __init__(self, weeks: int):
    if not isinstance(weeks, int):
      raise TypeError('Error: weeks must be of type <int>')
    if weeks < 1:
      raise TypeError('Error: weeks must be greater than 0')

    self.weeks: int = weeks
    self.daily_categories: pl.DataFrame = self._populate_daily_categories()

    self.df: pl.DataFrame | None = None
    self.day_plans: list[list[Any]] = []
  
  def add_daily_category(self, category: str, items: list[str], include_weekend: bool = False):
    """Adds a daily category with checklist items
    
    Args:
      category (str): the name of the category
      items (list[str]): the checklist items for the category
      include_weekend (optional) (bool): if True, add the checklists on Sunday and Saturday (default False)
    
    Updates:
      self.daily_categories (pl.DataFrame): the total daily categories for each day
    """
    self.daily_categories: pl.DataFrame = self.daily_categories \
      .with_columns([
        pl.when(include_weekend).then(pl.lit(items))
          .when(~pl.col('day').is_in(['Sun', 'Sat'])).then(pl.lit(items))
          .otherwise(pl.lit([]))
          .alias(category)
      ])
    
  def add_series(self, table: DataTable, include_weekend: bool = False, sort_by: str = ''):
    """Adds a book series to the daily reading plan list

    Args:
      table (DataTable): the series as a DataTable
      include_weekend (optional) (bool): if True, includes Sunday and Saturday (default False)
      sort_by (optional) (bool): if True, the column by which to sort the books (default '')
    
    Updates:
      self.day_plans (list[list[Any]]): adds each day's reading chunk for the series
    """
    if not isinstance(table, DataTable):
      raise TypeError('Error: table must be of type <DataTable>')
    if not isinstance(include_weekend, bool):
      raise TypeError('Error: include_weekend must be of type <bool>')
    if not isinstance(sort_by, str):
      raise TypeError('Error: sort_by must be of type <str>')
    if sort_by and sort_by not in table.columns:
      print(sort_by)
      print(table.columns)
      raise ValueError('Error: sort_by must be a valid column in table')

    # Retrieve the DataTable as a DataFrame
    df: pl.DataFrame = table.get()

    if sort_by:
      df: pl.DataFrame = df.sort(sort_by)

    if df.is_empty():
      raise RuntimeError(f'Error: The {table.table_name} DataTable is empty.')

    # Figure out how much per day to read and add each page to a list of total pages
    day_count: int = 7 if include_weekend else 5

    total_pages: int = df \
      .select(pl.sum('pages').alias('pages')) \
      .to_dicts() \
      [0] \
      ['pages']

    pages_per_week: int = ceil(total_pages / self.weeks)
    pages_per_day: int = ceil(pages_per_week / day_count)
    pages: list[dict[str, Any]] = []

    # Iterate the books in the table and get each page
    for book in df.to_dicts():
      for page in range(book['page_start'], book['page_end'] + 1):
        pages.append({
          'book': book['book'],
          'page': page
        })
    
    # Chunk the pages according to the daily goal
    chunks: list[list[Any]] = self._chunk_list(pages, pages_per_day)

    # Add the week and day for each chunk as metadata
    for chunk_index, chunk in enumerate(chunks):
      week: int = floor((chunk_index + day_count) / day_count)
      day: int = ((chunk_index) % day_count) + 1
      if not include_weekend:
        day += 1
      for day_part in chunk:
        day_part['week'] = week
        day_part['day'] = day

    # Add the chunks to the overall day plan list
    self.day_plans += chunks

  def build_reading_plan(self):
    """Based on the series added, creates a reading plan
    
    Updates:
      self.df (pl.DataFrame): contains the week, day, and readings
    """
    plan: list[dict[str, Any]] = []

    # Iterate the day plans and add them to the plan rows
    for day_plan in self.day_plans:
      day_df: pl.DataFrame = pl.DataFrame(day_plan)
      rows: dict[str, Any] = day_df.group_by('book', 'week', 'day').agg([
        pl.min('page').alias('start_page'),
        pl.max('page').alias('end_page')
      ]).to_dicts()

      for row in rows:
        plan.append(row)

    # Build the canonical reading plan as a DataFrame
    self.df: pl.DataFrame = pl.DataFrame(plan) \
      .select([
        pl.col('week'), 
        pl.col('day'), 
        pl.struct(['book', 'start_page', 'end_page']).alias('books')
      ]) \
      .group_by('week', 'day') \
      .agg([
        pl.col('books').alias('books')
      ]) \
      .sort('week', 'day') \
      .with_columns([
        pl.when(pl.col('day').eq(1)).then(pl.lit('Sun'))
          .when(pl.col('day').eq(2)).then(pl.lit('Mon'))
          .when(pl.col('day').eq(3)).then(pl.lit('Tue'))
          .when(pl.col('day').eq(4)).then(pl.lit('Wed'))
          .when(pl.col('day').eq(5)).then(pl.lit('Thu'))
          .when(pl.col('day').eq(6)).then(pl.lit('Fri'))
          .when(pl.col('day').eq(7)).then(pl.lit('Sat'))
        .alias('day')
      ])
    
    # If there are any daily categories, add them to self.df
    if len(self.daily_categories.columns) > 1:
      self.df: pl.DataFrame = self.df \
        .join(self.daily_categories, ['day'], 'left')
  
  def export_to_html(self, file_name: str = 'index'):
    """Creates a reading plan HTML page
    
    Args:
      file_name (optional) (str): the resulting file name (default 'index')
    """
    if not isinstance(file_name, str):
      raise TypeError('Error: file_name must be of type <str>')

    html_template: Template = Template("""
      <!DOCTYPE html>
      <html>
      <head>
          <style>
              body {
              font-family: Arial, sans-serif;
          }
          .page {
              page-break-after: always;
              display: flex;
              justify-content: space-between;
          }
          .column {
              border: 1px solid #000;
              margin: 0 5px;
          }
          table {
              width: 100%;
              border-collapse: collapse;
          }
          th, td {
              border: 1px solid #000;
              padding: 5px;
              text-align: left;
          }
          th {
              background-color: #f2f2f2;
          }
          </style>
      </head>
      <body>
          <table>
              <thead>
                  <tr>
                      <th>Week</th>
                      <th>Day</th>
                      <th>Readings</th>
                      {% for category in daily_categories %}
                      <th>{{ category }}</th>
                      {% endfor %}
                  </tr>
              </thead>
              <tbody>
              {% for row in data %}
                  <tr>
                      <td>{{ row['week'] }}</td>
                      <td>{{ row['day'] }}</td>
                      <td>
                          {% for book in row['books'] %}
                              <input type="checkbox" id="{{ book.book }}-{{ loop.index }}"> 
                              {{ book.book }} ({{ book.start_page }}-{{ book.end_page }})<br>
                          {% endfor %}
                      </td>
                      {% for category in daily_categories %}
                        <td>
                        {% for item in row[category] %}
                          <input type="checkbox" id="{{ category }}-{{ item }}"> 
                          {{ item }}
                          <br>
                        {% endfor %}
                        </td>
                      {% endfor %}
                  </tr>
              {% endfor %}
              </tbody>
          </table>
      </body>
      </html>
    """)
    rendered_template: str = html_template.render(
      data=self.df.to_dicts(),
      daily_categories=self.daily_categories.columns[1:]
    )

    open(f'{file_name}.html', 'w').write(rendered_template)
  
  def _chunk_list(self, lst: list[Any], max_size: int) -> list[list[Any]]:
    """Splits a list into chunks by max_size
    
    Args:
      lst (list[Any]): generic list to chunk
      max_size (int): the number of elements in each chunk
    """
    if not isinstance(lst, list):
      raise TypeError('Error: lst must be of type <list>')
    if not isinstance(max_size, int):
      raise TypeError('Error: max_size must be of type <int>')
    if max_size < 1:
      raise ValueError('Error: max_size must be greater than 0')
    
    return [lst[i:i + max_size] for i in range(0, len(lst), max_size)]

  def _populate_daily_categories(self) -> pl.DataFrame:
    """Populates initial days for the daily categories DataFrame
    
    Returns:
      daily_categories (pl.DataFrame): the daily categories DataFrame
    """
    return pl.DataFrame(data=[
      {'day': 'Sun'},
      {'day': 'Mon'},
      {'day': 'Tue'},
      {'day': 'Wed'},
      {'day': 'Thu'},
      {'day': 'Fri'},
      {'day': 'Sat'}
    ])
