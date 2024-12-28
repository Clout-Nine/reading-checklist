from datetime import datetime
from deltalake import DeltaTable, Schema
from enum import Enum
from json import loads
from os import getcwd, makedirs, path
from pathlib import Path
from polars import (
    col,
    DataFrame,
    lit,
    read_delta
)
from polars.datatypes import (
    Boolean,
    DataType,
    Date,
    Datetime,
    Float32,
    Float64,
    Int32,
    Int64,
    List,
    String
)


class DataTable:
    """Class to store all applicable information for a Delta Table"""

    # Constant variable for mapping Delta Lake field types to polars data types
    POLARS_FIELD_MAP: dict[str, DataType] = {
        'boolean': Boolean,
        'date': Date,
        'double': Float64,
        'float': Float32,
        'integer': Int32,
        'long': Int64,
        'list[str]': List(String),
        'list[int]': List(Int32),
        'list[long]': List(Int64),
        'string': String,
        'timestamp': Datetime(time_zone='America/New_York')
    }
    # Constant variables for data directories
    METASTORE_DIRECTORY: str = path.join(getcwd(), 'metastore')
    TABLES_DIRECTORY: str = path.join(METASTORE_DIRECTORY, 'tables')
    VOLUMES_DIRECTORY: str = path.join(METASTORE_DIRECTORY, 'volumes')

    # Sub-classes for delta table management
    class LoadLogic(Enum):
        APPEND: str = 'append'
        UPSERT: str = 'upsert'
        OVERWRITE: str = 'overwrite'
    
    class OutputFormat(Enum):
        DICT: str = 'dict'
        DICTS: str = 'dicts'
        DF: str = 'df'

    class SnapshotFormat(Enum):
        CSV: str = '.csv'
        EXCEL: str = '.xlsx'


    def __init__(self, catalog_name: str, schema_name: str, table_name: str, schema: Schema, 
                 primary_keys: list[str] = [], source_alias: str = 's', target_alias: str = 't'):
        if not isinstance(catalog_name, str):
            raise TypeError('Error: catalog_name must be of type <str>')
        if not isinstance(schema_name, str):
            raise TypeError('Error: schema_name must be of type <str>')
        if not isinstance(table_name, str):
            raise TypeError('Error: table_name must be of type <str>')
        if not isinstance(schema, Schema):
            raise TypeError('Error: schema must be of type <Schema>')
        if not isinstance(primary_keys, list):
            raise TypeError('Error: primary_keys must be of type <list>')
        if not isinstance(source_alias, str):
            raise TypeError('Error: source_alias must be of type <str>')
        if not isinstance(target_alias, str):
            raise TypeError('Error: target_alias must be of type <str>')
        if not all(isinstance(k, str) for k in primary_keys):
            raise ValueError('Error: all values in primary_keys must be of type <str>')
        if source_alias == target_alias:
            raise ValueError('Error: source_alias and target_alias must be different values')

        # Store each segment of the table path
        self.catalog_name: str = catalog_name
        self.schema_name: str = schema_name
        self.table_name: str = table_name

        # Store the absolute table path of data and state file
        self.table_path: str = path.join(
            DataTable.TABLES_DIRECTORY,
            self.catalog_name,
            self.schema_name,
            self.table_name
        )
        self.state_file_directory: str = path.join(
            DataTable.VOLUMES_DIRECTORY,
            'state',
            self.catalog_name,
            self.schema_name,
            self.table_name            
        )
        self.state_file_path: str = path.join(
            self.state_file_directory,
            '.STATE'
        )

        # Store the actual table columns and primary key restraints
        self.primary_keys: list[str] = primary_keys
        self.schema: Schema = schema
        self.schema_polars: dict[str, DataType] = self.get_schema_as_polars_schema(schema)
        self.columns: list[str] = list(self.schema_polars.keys())

        # Store the aliases for the merge logic
        self.source_alias: str = source_alias
        self.target_alias: str = target_alias

    @staticmethod
    def create_if_not_exists(table_path: str, schema: Schema):
        """Creates a Delta Table if it does not exist

        Args:
            table_path (str): the path of the Delta Table
            schema (Schema): the table schema, in case the Delta Table needs to be created
        """
        if not isinstance(table_path, str):
            raise TypeError('Error: table_path must be of type <str>')
        if not isinstance(schema, Schema):
            raise TypeError('Error: schema must be of type <Schema>')

        # If the Delta Table does not exist yet, create it with the expected schema             
        if not path.exists(table_path):
            makedirs(
                name=table_path,
                exist_ok=True
            )

            DeltaTable.create(
                table_uri=table_path,
                schema=schema
            )

    @staticmethod
    def get_schema_as_polars_schema(schema: Schema) -> dict:
        """Converts the existing Delta Lake schema and returns it as a dict compatible to Polars DataFrames

        Returns:
            schema_polars (dict[str, DataType]): the schema as a dict, compatible with Polars DataFrames
        """
        polars_schema: dict[str, DataType] = {}

        for field in loads(schema.to_json())['fields']:
            polars_schema[field['name']] = DataTable.POLARS_FIELD_MAP[field['type']]
            if field['type'] == 'timestamp':
                polars_schema[field['name']]
        
        return polars_schema

    def get(self, filter_conditions: dict = {}, output_format: OutputFormat = OutputFormat.DF, 
            partition_by: list[str] = [], order_by: list[str] = [], order_by_descending: bool = True, 
            select: list[str] = [], sort_by: list[str] = [], limit: int = 0,
            unique: bool = False) -> DataFrame | list[dict] | dict:
        """Retrieves a record, or records, by a specific condition, expecting only one record to return
        
        Args:
            filter_conditions (optional) (dict): if applicable, the filter conditions (e.g., {file_path: 'path.csv'})
            output_format (optional) (DataTable.OutputFormat): if applicable, the output format of the records
            partition_by (optional) (list[str]): if applicable, the keys by which to partition during deduplication
            order_by (optional) (list[str]): if applicable, the columns by which to order during deduplication
            order_by_descending (optional) (bool): if applicable, whether to ORDER BY DESC
            select (optional) (list[str]): if applicable, the columns to return after retrieving the DataFrame
            sort_by (optional) (list[str]): if applicable, the columns by which to sort the output
            limit (optional) (int): if applicable, a limit to the number of rows to return
            unique (optional) (bool): if applicable, remove any duplicate records
 
        Returns:
            records (list[dict]): the resulting records as a key-value pair
        """
        if not isinstance(filter_conditions, dict):
            raise TypeError('Error: filter_conditions must be of type <dict>')
        if not isinstance(output_format, DataTable.OutputFormat):
            raise TypeError('Error: output_format must be of type <DataTable.OutputFormat>')
        if not isinstance(partition_by, list):
            raise TypeError('Error: partition_by must be of type <list>')
        if not isinstance(order_by, list):
            raise TypeError('Error: order_by must be of type <list>')
        if not isinstance(select, list):
            raise TypeError('Error: select must be of type <list>')
        if not isinstance(sort_by, list):
            raise TypeError('Error: sort_by must be of type <list>')
        if not isinstance(limit, int):
            raise TypeError('Error: limit must be of type <int>')
        if not isinstance(unique, bool):
            raise TypeError('Error: unique must be of type <bool>')
        if not all(isinstance(c, str) for c in partition_by):
            raise ValueError('Error: all values in partition_by must be of type <str>')
        if not all(isinstance(c, str) for c in order_by):
            raise ValueError('Error: all values in order_by must be of type <str>')
        if not all(isinstance(c, str) for c in select):
            raise ValueError('Error: all values in select must be of type <str>')
        if not all(isinstance(c, str) for c in sort_by):
            raise ValueError('Error: all values in sort_by must be of type <str>')

        # Create the Delta Table if it does not exist
        if not path.exists(self.table_path):
            self.create_if_not_exists(self.table_path, self.schema)
           
        # Retrieve Delta Table as a Polars DataFrame
        df: DataFrame = read_delta(self.table_path)

        # Apply the filter condition if applicable
        if filter_conditions:
            df: DataFrame = df.filter(**filter_conditions)

        # Filter columns if applicable
        if select:
            df: DataFrame = df.select(select)

        # Apply a simple deduplication if applicable        
        if partition_by and order_by:
            df: DataFrame = df \
                .sort(order_by, descending=order_by_descending) \
                .unique(subset=partition_by, keep='first')
        
        # Apply a limit if applicable
        if limit:
            df: DataFrame = df.limit(limit)
        
        # Remove duplicate records if applicable
        if unique:
            df: DataFrame = df.unique()
          
        # Sort the results if applicable
        if sort_by:
            df: DataFrame = df.sort(sort_by)

        # After applying all of the manipulation logic, return data in the desired format
        if output_format == DataTable.OutputFormat.DICTS:
            # Return as a list of dicts
            return df.to_dicts()

        elif output_format == DataTable.OutputFormat.DICT:
            # Return as a single dict
            records: list[dict] = df.to_dicts()
            if not records:
                return {}
            if len(records) > 1:
                raise ValueError(f'Error: result is not 1 record: {records}')
            return records[0]

        elif output_format == DataTable.OutputFormat.DF:
            # Return as a DataFrame
            return df

        else:
            raise NotImplementedError(f'Error: output format not recognized: {output_format}')
        
    def does_file_path_exist(self, file_path: str) -> bool:
        """Indicates whether the file path exists in the Delta Table at all
        
        Args:
            file_path (str): the path to the file
        
        Returns:
            does_file_path_exist (bool): indicates whether the file path exists in the Delta Table
        """
        if not isinstance(file_path, str):
            raise TypeError('Error: file_path must be of type <str>')
        
        return self.get(output_format=DataTable.OutputFormat.DF) \
            .filter(col('_file_path').eq(lit(file_path))) \
            .shape[0] > 0

    def build_merge_predicate(self) -> str:
        """Constructs a merge predicate based on the source/target aliases and primary keys

        Returns:
            merge_predicate (str): the merge predicate as a conjunction of SQL conditions matching on primary keys
        """
        if not self.primary_keys:
            raise ValueError('Error: Delta Table is missing primary keys')

        return ' AND '.join([f'{self.source_alias}.{k} = {self.target_alias}.{k}' for k in self.primary_keys])

    def export_snapshot(self, format: SnapshotFormat, tag: str = '', include_timestamp: bool = False):
        if not isinstance(format, DataTable.SnapshotFormat):
            raise TypeError('Error: format must be of type <DataTable.SnapshotFormat>')
        
        # Retrieve the current Delta Table data as a DataFrame
        df: DataFrame = self.get()

        # Build output directory
        output_directory: str = path.join('build', 'snapshots')
        makedirs(output_directory, exist_ok=True)

        # Build output file name and path
        output_file_name: str = '_'.join([
            self.catalog_name,
            self.schema_name,
            self.table_name
        ])
        if tag:
            output_file_name += f'_{tag}'
        if include_timestamp:
            output_file_name += f'_{datetime.now().strftime("%Y%m%d%H%M%S")}'
        output_file_name += format.value
        output_file_path: str = path.join(output_directory, output_file_name)
        
        # Save the snapshot in the desired format
        if format == DataTable.SnapshotFormat.CSV:
            df.to_pandas().to_csv(output_file_path)
        elif format == DataTable.SnapshotFormat.EXCEL:
            df.to_pandas().to_excel(output_file_path)
        else:
            raise NotImplementedError(f'Error: format not recognized: {format}')

        print(f'[{datetime.now()}] The snapshot has been saved: {output_file_path}')

    def append_record(self, record: dict[str, any]):
        """Saves a single record to the underlying Delta Table
        
        Args:
          record (dict[str, any]): a record in the expected schema
        """
        df: DataFrame = DataFrame(
            data=[record],
            schema=self.schema_polars
        )
        self.save_to_delta(
            df=df,
            load_logic=DataTable.LoadLogic.APPEND
        )

    def save_to_delta(self, df: DataFrame, load_logic: LoadLogic = LoadLogic.UPSERT):
        """Saves an inputted DataFrame to its target Delta Table based on an inputted load logic
        
        Args:
            df (DataFrame): a compatible DataFrame with this schema
            load_logic (LoadLogic): the specific load logic to use against the target Delta Table
        """
        if not isinstance(load_logic, self.LoadLogic):
            raise TypeError('Error: load_logic must be of type <LoadLogic>')
        
        DataTable.create_if_not_exists(
            table_path=self.table_path,
            schema=self.schema
        )
        
        if load_logic == self.LoadLogic.UPSERT:
            if not self.primary_keys:
                raise ValueError('Error: Delta Table does not have primary keys')

            # Merge the DataFrame into its respective Delta Table
            # This merge logic is a simple upsert based on the table's primary keys
            df \
                .write_delta(
                    target=self.table_path,
                    mode='merge',
                    delta_merge_options={
                        'predicate': self.build_merge_predicate(),
                        'source_alias': self.source_alias,
                        'target_alias': self.target_alias,
                    }
                ) \
                .when_matched_update_all() \
                .when_not_matched_insert_all() \
                .execute()
        elif load_logic == self.LoadLogic.APPEND:
            df.write_delta(
                target=self.table_path,
                mode='append'
            )
        elif load_logic == self.LoadLogic.OVERWRITE:
            df.write_delta(
                target=self.table_path,
                mode='overwrite'
            )
        else:
            raise NotImplementedError(f'[ERROR] Load logic \'{load_logic}\' not implemented')

        self.touch_state_file()

    def touch_state_file(self):
        """Touches the state file to update the last modified datetime"""
        try:
          Path(self.state_file_path).touch()
        except FileNotFoundError:
          makedirs(self.state_file_directory, exist_ok=True)        
          Path(self.state_file_path).touch()

    def get_last_modified_datetime(self) -> datetime:
        """Retrieves the last modified datetime of the table
        
        Returns:
          last_modified_datetime (datetime): the last modified datetime of the table
        """
        modified_time: float = path.getmtime(self.state_file_path)
        return datetime.fromtimestamp(modified_time)

    def get_last_modified_record(self) -> dict[str, any]:
        """Builds a dict record for last modified information

        Returns:
          last_modified_record (dict[str, any]): the metadata of the table's last modified timestamp
        """
        return {
          'catalog': self.catalog_name,
          'schema': self.schema_name,
          'table': self.table_name,
          'last_modified_datetime': self.get_last_modified_datetime()
        }
