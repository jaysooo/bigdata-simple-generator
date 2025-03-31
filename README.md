# bigdata-simple-generator

빅데이터 생성기입니다. 

PyArrow를 사용하여 CSV, Parquet, JSON 형식의 데이터를 생성할 수 있습니다.

인터페이스를 참고하여 Pyspark , pandas 등의 다양한 Dataframe API 를 이용하여 생산자를 직접 구현할 수 있습니다.

ETL 데이터 프로그램, 다양한 벤치마크 실험을 위해 더미 데이터가 필요할 때 사용해보세요.



## 패키지 구조

```
bigdata-simple-generator/
├── producer/                 # 데이터 생성 및 저장 관련 모듈
│   ├── interface.py          # Producer 추상 클래스 정의
│   └── pyarrow_producer.py   # PyArrow를 사용한 데이터 생성 구현
├── utils/                    # 유틸리티 함수 모듈
│   └── rand.py               # 랜덤 데이터 생성 함수
├── config.yaml               # 데이터 스키마 및 설정 파일
├── data-generator.py         # 메인 실행 스크립트
└── requirement.txt           # 필요 패키지 목록
```

## 설치 방법

1. 저장소를 클론합니다.
2. 필요한 패키지를 설치합니다:

```bash
pip install -r requirement.txt
```

## 실행 예제

기본 설정으로 실행:

```bash
python data-generator.py --config config.yaml --producer pyarrow
```

## 설정 파일 (config.yaml)

설정 파일에서 다음을 지정할 수 있습니다:
- 테이블 이름, 레코드 수
- 파일 형식 (csv, parquet, json)
- 출력 저장 경로
- 파티션 키 및 범위
- 테이블 스키마 (컬럼 이름, 타입, 범위 등)

예시:
```yaml
data_spec:
  table_name: stg-event-dummy
  records: 1000
  file_format: csv
  file_prefix: data
  destination: /path/to/output
  partition_by:
    name: partition_key
    range:
      min: "2025-01-01"
      max: "2025-01-10"
```

---

# Simple Data Generator

A simple dummy data generator. You can generate data in CSV, Parquet, or JSON format using PyArrow.

## Package Structure

```
bigdata-simple-generator/
├── producer/                 # Data generation and storage modules
│   ├── interface.py          # Producer abstract class definition
│   └── pyarrow_producer.py   # Data generation implementation using PyArrow
├── utils/                    # Utility function modules
│   └── rand.py               # Random data generation functions
├── config.yaml               # Data schema and configuration file
├── data-generator.py         # Main execution script
└── requirement.txt           # Required packages list
```

## Installation

1. Clone the repository.
2. Install the required packages:

```bash
pip install -r requirement.txt
```

## Execution Example

Run with default settings:

```bash
python data-generator.py --config config.yaml --producer pyarrow
```

## Configuration File (config.yaml)

In the configuration file, you can specify:
- Table name, number of records
- File format (csv, parquet, json)
- Output storage path
- Partition key and range
- Table schema (column names, types, ranges, etc.)

Example:
```yaml
data_spec:
  table_name: stg-event-dummy
  records: 1000
  file_format: csv
  file_prefix: data
  destination: /path/to/output
  partition_by:
    name: partition_key
    range:
      min: "2025-01-01"
      max: "2025-01-10"
```
