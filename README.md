# NexR_dqc
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
- documents 하위 항목은 필수로 작성되어야 함
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

