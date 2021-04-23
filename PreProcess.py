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
            self.tableDocs = pd.read_excel(os.path.join(docsPath, list(file for file in os.listdir(docsPath) if "테이블" in file)[0]))
            self.colDocs = pd.read_excel(os.path.join(docsPath, list(file for file in os.listdir(docsPath) if "컬럼" in file)[0]))
            self.codeDocs = pd.read_excel(os.path.join(docsPath, list(file for file in os.listdir(docsPath) if "코드" in file)[0]))
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
        self.na_check()
        # data의 row, column 수를 확인한다.
        rows, columns = self.data.shape
        # data 내 전체 결측값을 확인한다.
        total_null = sum(self.data.isnull().sum())
        # 중복되는 row가 있는지 확인한다.
        duplicate_row = sum(self.data.duplicated())
        # 중복 row의 index를 확인한다.
        duplicate_index = [
            idx
            for idx, result in self.data.duplicated().to_dict().items()
            if result is True
        ]
        # 연속형 변수를 확인한다.
        numeric_var = list(self.data.select_dtypes(include=np.number).columns)
        num = dict({"count": len(numeric_var), "variables": numeric_var})
        # 범주형 변수를 확인한다.
        string_var = list(self.data.select_dtypes(include=np.object).columns)
        string = dict({"count": len(string_var), "variables": string_var})
        self.overview["dataset"] = {
            "rows": rows,
            "cols": columns,
            "null": total_null,
            "null%": round(total_null / rows, 2),
            "numeric_var": num,
            "string_var": string,
            "duplicate_row": duplicate_row,
            "duplicate_row_index": duplicate_index,
        }
        # 각 변수 summary값 dict 형태로 저장한다.
        self.eda_result = dict()
        self.eda_result["num"] = dict()
        self.eda_result["str"] = dict()
        for column_name in self.data.columns:
            if column_name in self.overview["dataset"]["numeric_var"]["variables"]:
                summary = self.data[column_name].describe()
                # json으로 저장하기 위해 형식을 변경한다. 
                for i in summary.keys():
                    summary[i] = float(summary[i])
                summary["null_count"] = self.data[column_name].isnull().sum()
                summary["null_percent"] = summary["null_count"] / len(self.data)
                summary["all_null"] = (1 if summary["null_percent"] == 1 else 0)
                self.eda_result["num"][column_name] = dict(summary)
            elif column_name in self.overview["dataset"]["string_var"]["variables"]:
                ftable = dict(self.data[column_name].value_counts())
                ftable_per = dict(self.data[column_name].value_counts()/len(self.data))
                # json으로 저장하기 위해 형식을 변경한다. 
                for i in ftable.keys():
                    ftable[i] = int(ftable[i])
                for i in ftable_per.keys():
                    ftable_per[i] = float(ftable_per[i])
                summary = dict({"class": ftable})
                summary['class_percent'] = ftable_per
                summary["null_count"] = int(self.data[column_name].isnull().sum())
                summary["null_percent"] = summary["null_count"] / len(self.data)
                summary["all_null"] = (1 if summary["null_percent"] == 1 else 0)
                self.eda_result["str"][column_name] = dict(summary)

    def dqc(self):
        # dqc table 출력하기
        column = [
            ["컬럼"] * 3 + ["연속형 대상"] * 4 + ["범주형 대상"] * 4 + ["공통"] * 6,
            ["컬럼명", "한글명", "타입", "최소값", "최대값", "평균", "표준편차", "범주 수", "범주", "범주%", 
            "정의된 범주 외", "최빈값", "NULL값", "NULL수", "NULL%", "적재건수", "적재건수%"]
        ]
        self.result = pd.DataFrame(columns=column)
        for column_type in self.eda_result.keys():
            for column_name in self.eda_result[column_type].keys():
                data_summary = self.eda_result[column_type][column_name]
                if column_type == "num":
                    temp_df = pd.DataFrame(
                        np.array(
                            (
                                column_name,
                                column_type,
                                round(data_summary["min"], 2),
                                round(data_summary["max"], 2),
                                round(data_summary["mean"], 2),
                                round(data_summary["std"], 2),
                                data_summary["null_count"],
                                round(data_summary["null_percent"] * 100, 2),
                                data_summary["count"],
                                round(data_summary["count"] / len(self.data) * 100, 2)
                            )
                        ).reshape(1, 10),
                        columns=[
                            ["컬럼"] * 2 + ["연속형 대상"] * 4 + ["공통"] * 4,
                            ["컬럼명", "타입", "최소값", "최대값", "평균", "표준편차", "NULL수", "NULL%", "적재건수", "적재건수%"]
                        ],
                    )
                else:
                    # class percent 자리수 해결하는 코드
                    class_percent_tmp = dict(zip(list(data_summary['class_percent'].keys()), np.round(list(data_summary['class_percent'].values()),2)))
                    temp_df = pd.DataFrame(
                        np.array(
                            (
                                column_name,
                                column_type,
                                len(data_summary["class"]),
                                ", ".join(list(data_summary["class"].keys())),
                                str(list(class_percent_tmp.items())[:5]).replace("[","").replace("]",""),
                                data_summary["null_count"],
                                round(data_summary["null_count"] / len(self.data) * 100, 2),
                                len(self.data) - data_summary["null_count"],
                                100 - round(data_summary["null_count"] / len(self.data) * 100, 2)
                            )
                        ).reshape(1, 9),
                        columns=[
                            ["컬럼"] * 2 + ["범주형 대상"] * 3 + ["공통"] * 4,
                            ["컬럼명", "타입", "범주 수", "범주", "범주%", "NULL수", "NULL%", "적재건수", "적재건수%"]
                        ],
                    )

                self.result = pd.concat(
                    [self.result, temp_df], ignore_index=True
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