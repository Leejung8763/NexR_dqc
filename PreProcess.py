import os
import json
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# # 과학적 표기법(Scientific notation)을 사용하지 않는 경우
# pd.options.display.float_format = "{:.2f}".format

class PreProcess:

    def __init__(self, inputPath, docsPath):
        # 공통 결측치를 설정한다. 
        self.naList = ["?", "na", "null", "Null", "NULL", " "]
        addNa = input(
            f"결측값을 추가할 수 있습니다. \n기본 설정된 결측값: {self.naList} \n추가하고 싶은 결측값을 작성해주십시오:"
        )
        if len(addNa) != 0:
            self.naList += addNa.replace(" ", "").split(sep=",")
            self.naList = list(set(self.naList))
        print(f"현재 공통 결측값 리스트는 {self.naList} 입니다.")
        fileName = inputPath.split('/')[-1]
        try:
            if ".csv" in inputPath:
                self.data = pd.read_csv(inputPath, na_values=self.naList)
                self.fileName = fileName.split('.csv')[0]            
            elif ".parquet" in inputPath:
                self.data = pq.read_pandas(inputPath).to_pandas()
                self.fileName = fileName.split('.parquet')[0]
            # 테이블 정의서
            self.tableDocs = pd.read_excel(os.path.join(docsPath, list(file for file in os.listdir(docsPath) if "테이블" in file)[0]), usecols=lambda x: 'Unnamed' not in x)
            self.tableDocs.columns = [i.replace("\n","") for i in self.tableDocs.columns]
            # 컬럼 정의서
            self.colDocs = pd.read_excel(os.path.join(docsPath, list(file for file in os.listdir(docsPath) if "컬럼" in file)[0]), usecols=lambda x: 'Unnamed' not in x)
            self.colDocs.columns = [i.replace("\n","") for i in self.colDocs.columns]
            self.colDocs = pd.merge(self.colDocs, self.tableDocs[["시스템명(영문)", "스키마명" , "테이블명(영문)", "DB 유형"]], on=["시스템명(영문)", "스키마명" , "테이블명(영문)"], how="left")
            self.colDocs["데이터타입"] = self.colDocs["데이터타입"].str.upper()
            # 코드 정의서
            self.codeDocs = pd.read_excel(os.path.join(docsPath, list(file for file in os.listdir(docsPath) if "코드" in file)[0]), usecols=lambda x: 'Unnamed' not in x)
            # 데이터 타입 정의서
            self.dtypeDocs = pd.read_excel(os.path.join(docsPath, list(file for file in os.listdir(docsPath) if "Datatype" in file)[0]), usecols=lambda x: 'Unnamed' not in x)
            self.dtypeDocs["DataType"] = self.dtypeDocs["DataType"].str.upper()
            # 데이터 형식 변환
            self.colDocs = pd.merge(self.colDocs, self.dtypeDocs, left_on=["데이터타입","DB 유형"], right_on=["DataType", "DBMS"], how="left")
            self.colDocs = self.colDocs.drop(["NewDataType","DBMS","DataType"], axis=1)
            self.colDocs = self.colDocs.drop_duplicates()
            dbType = dict(zip(self.colDocs.loc[self.colDocs["테이블명(영문)"]==self.fileName, '컬럼명'], self.colDocs.loc[self.colDocs["테이블명(영문)"]==self.fileName, 'PyDataType']))
            self.data = self.data.astype(dbType, errors="ignore")
            self.overview = dict()
        except:
            print("해당 파일이 존재하지 않습니다. 경로를 확인하세요.")    

    def na_check(self):
        # 공통 결측치를 설정한다. 
        self.naList = ["?", "na", "null", "Null", "NULL", " "]
        addNa = input(
            f"결측값을 추가할 수 있습니다. \n기본 설정된 결측값: {self.naList} \n추가하고 싶은 결측값을 작성해주십시오:"
        )
        if len(addNa) != 0:
            self.naList += addNa.replace(" ", "").split(sep=",")
            self.naList = list(set(self.naList))
        # 결측값을 처리한다. 
        self.data[self.data.isin(self.naList)] = np.nan
        
    def eda(self):
        # data의 row, column 수를 확인한다.
        rows, columns = self.data.shape
        # data 내 전체 결측값을 확인한다.
        totalNull = sum(self.data.isnull().sum())
        # 중복되는 row가 있는지 확인한다.
        duplicateRow = sum(self.data.duplicated())
        # 중복 row의 index를 확인한다.
        duplicateIdx = [
            idx
            for idx, result in self.data.duplicated().to_dict().items()
            if result is True
        ]
        # 연속형 변수를 확인한다.
        numericVar = list(self.data.select_dtypes(include=np.number).columns)
        num = dict({"count": len(numericVar), "variables": numericVar})
        # 범주형 변수를 확인한다.
        stringVar = list(self.data.select_dtypes(include="string").columns)
        string = dict({"count": len(stringVar), "variables": stringVar})
        self.overview["dataset"] = {
            "rows": rows,
            "cols": columns,
            "null": totalNull,
            "null%": round(totalNull / rows, 2),
            "numericVar": num,
            "stringVar": string,
            "duplicateRow": duplicateRow,
            "duplicateRowIdx": duplicateIdx,
        }
       # 각 변수 summary값 dict 형태로 저장한다.
        self.edaResult = dict()
        self.edaResult["num"] = dict()
        self.edaResult["str"] = dict()
        for columnName in self.data.columns:
            if columnName in self.overview["dataset"]["numericVar"]["variables"]:
                summary = dict({"korName":self.colDocs.loc[(self.colDocs["테이블명(영문)"]==self.fileName)&(self.colDocs["컬럼명"]==columnName),"속성명(컬럼한글명)"].unique()[0]})
                summaryTmp = self.data[columnName].describe()
                # json으로 저장하기 위해 형식을 변경한다. 
                for i in summaryTmp.keys():
                    summary[i] = float(summaryTmp[i])
                summary["count"] = len(self.data[columnName])
                summary["nullCount"] = self.data[columnName].isnull().sum()
                summary["nullProp"] = summary["nullCount"] / len(self.data)
                summary["nullOnly"] = (1 if summary["nullProp"] == 1 else 0)
                self.edaResult["num"][columnName] = dict(summary)
            elif columnName in self.overview["dataset"]["stringVar"]["variables"]:
                summary = dict({"korName":self.colDocs.loc[(self.colDocs["테이블명(영문)"]==self.fileName)&(self.colDocs["컬럼명"]==columnName),"속성명(컬럼한글명)"].unique()[0]})
                summary["count"] = len(self.data[columnName])
                ftable = dict(self.data[columnName].value_counts())
                ftableProp = dict(self.data[columnName].value_counts()/len(self.data))
                # json으로 저장하기 위해 형식을 변경한다. 
                for i in ftable.keys():
                    ftable[i] = int(ftable[i])
                for i in ftableProp.keys():
                    ftableProp[i] = float(ftableProp[i])
                summary["class"] = ftable
                summary['classProp'] = ftableProp
                summary["nullCount"] = int(self.data[columnName].isnull().sum())
                summary["nullProp"] = summary["nullCount"] / len(self.data)
                summary["nullOnly"] = (1 if summary["nullProp"] == 1 else 0)
                self.edaResult["str"][columnName] = dict(summary)
                
    def dqc(self):
        # dqc table 출력하기
        column = [
            ["컬럼"] * 3 + ["연속형 대상"] * 7 + ["범주형 대상"] * 4 + ["공통"] * 5,
            ["컬럼명", "한글명", "타입", 
             "최소값", "25%", "50%", "75%", "최대값", "평균", "표준편차",
             "범주 수", "정의된 범주 외", "정의된 범주 외%", "최빈값",
             "NULL값", "NULL수", "NULL%", "적재건수", "적재건수%"]
        ]
        self.result = pd.DataFrame(columns=column)
        for columnType in self.edaResult.keys():
            for columnName in self.edaResult[columnType].keys():
                dataSummary = self.edaResult[columnType][columnName]
                if columnType == "num":
                    tempDf = pd.DataFrame(
                        np.array(
                            (
                                columnName,
                                dataSummary["korName"],
                                columnType,
                                round(dataSummary["min"], 2),
                                round(dataSummary["25%"], 2),
                                round(dataSummary["50%"], 2),
                                round(dataSummary["75%"], 2),
                                round(dataSummary["max"], 2),
                                round(dataSummary["mean"], 2),
                                round(dataSummary["std"], 2),
                                dataSummary["nullCount"],
                                round(dataSummary["nullProp"] * 100, 2),
                                dataSummary["count"],
                            )
                        ).reshape(1, 13),
                        columns=[
                            ["컬럼"] * 3 + ["연속형 대상"] * 7 + ["공통"] * 3,
                            ["컬럼명", "한글명", "타입", 
                            "최소값", "25%", "50%", "75%", "최대값", "평균", "표준편차", 
                            "NULL수", "NULL%", "적재건수"]
                        ],
                    )
                else:
                    # class proportion 자리수 해결하는 코드
                    classPropTmp = dict(zip(list(dataSummary['classProp'].keys()), np.round(list(dataSummary['classProp'].values()),2)))
                    tempDf = pd.DataFrame(
                        np.array(
                            (
                                columnName,
                                dataSummary["korName"],
                                columnType,
                                len(dataSummary["class"]),
                                (str(list(dataSummary['class'].items())[0]).replace(")", "").replace("(", "").replace(",", ":") if len(list(dataSummary['class'].items())) > 0 else np.nan),
                                dataSummary["nullCount"],
                                round(dataSummary["nullCount"] / len(self.data) * 100, 2),
                                dataSummary["count"],
                            )
                        ).reshape(1, 8),
                        columns=[
                            ["컬럼"] * 3 + ["범주형 대상"] * 2 + ["공통"] * 3,
                            ["컬럼명", "한글명", "타입",
                             "범주 수", "최빈값",
                             "NULL수", "NULL%", "적재건수"]
                        ],
                    )
                self.result = pd.concat(
                    [self.result, tempDf], ignore_index=True
                ).reindex(columns=column)

    def save(self, output_path):
        if not os.path.exists(os.path.join(output_path, self.file_name)):
            os.makedirs(os.path.join(output_path, self.file_name))
        else:
            print('지정된 저장폴더가 이미 존재합니다.')
            raise SystemExit           
            
        json.dump(self.overview, open(f"{os.path.join(output_path, self.file_name)}/overview.json", "w"))
        json.dump(self.eda_result, open(f"{os.path.join(output_path, self.file_name)}/eda_result.json", "w"))
        self.result.to_excel(f"{os.path.join(output_path, self.file_name)}/dqc_table.xlsx")

        print("저장완료")