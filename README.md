# NexR_dqc
[![PyPI](https://img.shields.io/pypi/v/NexR_dqc?style=plastic&color=blue)](https://pypi.org/project/NexR_dqc/)
<br><br>

## 요구사항
- python >= 3.7.1
- numpy
- pandas==1.2.4
- pyarrow
- openpyxl
- xlsxwriter
<br>

## 설치

### venv 설치 및 활성화 
```
virtualenv pre_process 

cd pre_process 
source bin/activate

pip install NexR_dqc
```

### 폴더 구성
- documents 하위 항목([DBMS유형별_Datatype](https://github.com/Leejung8763/NexR_dqc/raw/main/documents/DBMS%EC%9C%A0%ED%98%95%EB%B3%84_Datatype.xlsx), [테이블정의서](https://github.com/Leejung8763/NexR_dqc/raw/main/documents/%ED%85%8C%EC%9D%B4%EB%B8%94%EC%A0%95%EC%9D%98%EC%84%9C.xlsx), [컬럼정의서](https://github.com/Leejung8763/NexR_dqc/raw/main/documents/%EC%BD%94%EB%93%9C%EC%A0%95%EC%9D%98%EC%84%9C.xlsx), [코드정의서](https://github.com/Leejung8763/NexR_dqc/raw/main/documents/%EC%BB%AC%EB%9F%BC%EC%A0%95%EC%9D%98%EC%84%9C.xlsx))은 필수로 작성되어야 함
```
data/
├── documents/
│   ├── DBMS유형별_Datatype
│   ├── 테이블정의서
│   ├── 컬럼정의서
│   └── 코드정의서
└── data.csv
``` 
<br>

## 예제 실행 
```
import sys
from NexR_dqc import PreProcess 

inputData = './sample.csv'
docsPath = './documents'
outputPath = '.'

code = PreProcess.PreProcess(inputData, docsPath)

code.summary()
code.eda()
code.save(outputPath)
```
