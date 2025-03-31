from producer.interface import Producer
from utils.rand import *
import pyarrow as pa
from datetime import datetime, timedelta
import pyarrow.dataset as ds

class PyArrowProducer(Producer):
    def __init__(self, config: dict):
        self.config = config
        self.producer_type = 'pyarrow'

    def write(self,data:pa.Table) -> None:
        if len(data)> 0 :
            ds.write_dataset(
                data,
                base_dir=self.config['data_spec']['destination'],  # 저장 경로
                format=self.config['data_spec']['file_format'],
                partitioning=["partition_key"],
                existing_data_behavior='overwrite_or_ignore'
            )

    def write_by_simple(self,data:pa.Table) -> None:

        write_format = self.config['data_spec']['file_format']
        file_prefix = self.config['data_spec']['file_prefix']
        file_name = f"{file_prefix}_{datetime.now().strftime('%Y%m%d')}.{write_format}"
        default_batch_size = 10 
        if write_format == 'csv':
            from pyarrow import csv
            write_options = csv.WriteOptions(
                include_header=True,
                delimiter=',',
                batch_size=default_batch_size
            )
            csv.write_csv(data, f'{file_name}', write_options=write_options)

        elif write_format == 'parquet':
            from pyarrow import parquet
            parquet.write_table(data,f'{file_name}')
        elif write_format == 'json':
            from pyarrow import json
            json.write_json(data,f'{file_name}')

        else:
            raise ValueError(f"Unsupported format: {write_format}")

    def produce(self, data: pa.Table) -> None:
        start_partition = datetime.strptime(self.config['data_spec']['partition_by']['range']['min'], '%Y-%m-%d')
        end_partition = datetime.strptime(self.config['data_spec']['partition_by']['range']['max'], '%Y-%m-%d')
        partition_key = self.config['data_spec']['partition_by']['name']

        while start_partition < end_partition:
            write_data = self.add_partition_dataset(data,partition_key,start_partition.strftime('%Y-%m-%d'))
            self.write(write_data)
            start_partition += timedelta(days=1)


    def add_partition_dataset(self,data:pa.Table,partition_key:str , partition_value:str) -> pa.Table:  
        partition_array = pa.array([str(partition_value)] * len(data))
        data = data.append_column(partition_key, partition_array)

        return data
    

    def build_dataset(self):
        # dataset = pa.Table.from_arrays([])
        schema = self.config['data_spec']['table_schema']
        record_count = self.config['data_spec']['records']
        print(record_count)
        arrays = []
        names = []

        for col in schema:
            part_array = None
        
            if col["name"] == 'id':
                if 'prefix' in col:
                    part_array = pa.array([get_random_index_string(10, prefix=col['prefix']) for _ in range(record_count)])
                else:
                    part_array = pa.array([get_random_index_string(10) for _ in range(record_count)])
            elif col["name"] == "device_type":
                if "range" in col:
                    part_array = pa.array([get_random_string_from_item(col['range']['items']) for _ in range(record_count)])
                else:
                    part_array = pa.array([get_random_string(10) for _ in range(record_count)])
            elif col["type"] == 'string':
                if 'prefix' in col:
                    part_array = pa.array([get_random_string(10, prefix=col['prefix']) for _ in range(record_count)])
                else:
                    part_array = pa.array([get_random_string(10) for _ in range(record_count)])
            elif col["type"]  == 'timestamp':
                part_array = pa.array([get_random_timestamp() for _ in range(record_count)])
            elif col["type"]  == 'float':
                if 'range' in col:
                    arr = [get_random_float(min= col['range']['min'], max = col['range']['max'],decimal_point=col['decimal_point']) for _ in range(record_count)]
                else:
                    arr = [get_random_float(min= 0, max = 1,decimal_point=2) for _ in range(record_count)]
                part_array  = pa.array(arr, type = pa.float64())
            elif col["type"] == 'int':
                if 'range' in col:
                    arr = [get_random_int(min = col['range']['min'], max = col['range']['max']) for _ in range(record_count)]
                else:
                    arr = [get_random_int() for _ in range(record_count)]
                part_array  = pa.array(arr, type = pa.int64())
            else:
                raise ValueError(f"Unsupported type: {col['type']}")
            arrays += [part_array]
            names += [col["name"]]
        dataset = pa.Table.from_arrays(arrays, names=names)


   
        return dataset


