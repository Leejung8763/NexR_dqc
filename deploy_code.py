import os
import json
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# 과학적 표기법(Scientific notation)을 사용하지 않는 경우
pd.options.display.float_format = "{:.2f}".format


class dqc_code:
    def __init__(self, input_path):
        temp_file_name = input_path.split('/')[-1]
        try:
            if ".csv" in input_path:
                self.data = pd.read_csv(input_path)
                self.file_name = temp_file_name.split('.csv')[0]                
            elif ".parquet" in input_path:
                self.data = pq.read_pandas(input_path).to_pandas()
                self.file_name = temp_file_name.split('.parquet')[0]
            self.overview = dict()
        except:
            print("해당 파일이 존재하지 않습니다. 경로를 확인하세요.")        
#         try:
#             if ".csv" in input_path:
#                 self.data = pd.read_csv(input_path)
#                 self.file_name = temp_file_name.split('.csv')[0]                
#             elif ".parquet" in input_path:
#                 print('11')
#                 self.data = pq.read_pandas(input_path).to_pandas()
#                 self.file_name = temp_file_name.split('.parquet')[0]
#             self.overview = dict()
#         except:
#             print("해당 파일이 존재하지 않습니다. 경로를 확인하세요.")

    def na_check(self):
        # 공통 결측치
        self.na_list = ["?", "na", "null", "Null", "NULL", " "]
        add_na = input(
            f"결측값을 추가할 수 있습니다. \n기본 설정된 결측값: {self.na_list} \n추가하고 싶은 결측값을 작성해주십시오:"
        )
        if len(add_na) != 0:
            self.na_list += add_na.replace(" ", "").split(sep=",")
            self.na_list = list(set(self.na_list))
        # change all NA
        self.data[self.data.isin(self.na_list)] = np.nan

    def eda(self):
        # 결측치 체크
        self.na_check()
        # data 크기 저장.
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
        # 연속형 변수 리스트
        numeric_var = list(self.data.select_dtypes(include=np.number).columns)
        num = dict({"count": len(numeric_var), "variables": numeric_var})
        # 범주형 변수 리스트
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
        # 각 변수 summary값 dict 형태로 저장
        self.eda_result = dict()
        self.eda_result["num"] = dict()
        self.eda_result["str"] = dict()
        for cname in self.data.columns:
            if cname in self.overview["dataset"]["numeric_var"]["variables"]:
                summary = self.data[cname].describe()
                # json 저장이 안되기 때문에 형식 변경 필요.
                for i in summary.keys():
                    summary[i] = float(summary[i])
                summary["null_count"] = self.data[cname].isnull().sum()
                summary["null_percent"] = summary["null_count"] / len(self.data)
                summary["unique_count"] = self.data[cname].isnull().sum()
                summary["unique_percent"] = summary["unique_count"] / len(self.data)
                summary["all_null"] = (
                    1 if summary["null_percent"] == 1 else 0
                )  # 값 전체가 결측값인 column은 all_null 값이 1로 입력된다.
                summary["all_same"] = (
                    1 if summary["unique_count"] == 1 else 0
                )  # 값 전체가 동일한 column은 all_same 값이 1로 입력된다.
                self.eda_result["num"][cname] = dict(summary)
            elif cname in self.overview["dataset"]["string_var"]["variables"]:
                ftable = dict(self.data[cname].value_counts())
                # json 저장이 안되기 때문에 형식 변경 필요
                for i in ftable.keys():
                    ftable[i] = int(ftable[i])
                summary = dict({"class": ftable})
                summary["null_count"] = int(self.data[cname].isnull().sum())
                summary["null_percent"] = summary["null_count"] / len(self.data)
                summary["unique_count"] = int(self.data[cname].isnull().sum())
                summary["unique_percent"] = summary["unique_count"] / len(self.data)
                summary["all_null"] = (
                    1 if summary["null_percent"] == 1 else 0
                )  # 값 전체가 결측값인 column은 all_null 값이 1로 입력된다.
                summary["all_same"] = (
                    1 if summary["unique_count"] == 1 else 0
                )  # 값 전체가 동일한 column은 all_same 값이 1로 입력된다.
                self.eda_result["str"][cname] = dict(summary)

    def dqc(self):
        # dqc table 출력하기
        column = [
            ["컬럼"] * 3 + ["연속형 대상"] * 4 + ["범주형 대상"] * 4 + ["공통"] * 6,
            [
                "컬럼명",
                "한글명",
                "타입",
                "최소값",
                "최대값",
                "평균",
                "표준편차",
                "범주 수",
                "범주",
                "범주%",
                "정의된 범주 외",
                "최빈값",
                "NULL값",
                "NULL수",
                "NULL%",
                "적재건수",
                "적재건수%",
            ],
        ]
        self.result = pd.DataFrame(columns=column)
        for ctype in self.eda_result.keys():
            for cname in self.eda_result[ctype].keys():
                datasummary = self.eda_result[ctype][cname]
                if ctype == "num":
                    temp_df = pd.DataFrame(
                        np.array(
                            (
                                cname,
                                ctype,
                                round(datasummary["min"], 2),
                                round(datasummary["max"], 2),
                                round(datasummary["mean"], 2),
                                round(datasummary["std"], 2),
                                datasummary["null_count"],
                                round(
                                    datasummary["null_percent"] * 100,
                                    2,
                                ),
                                datasummary["count"],
                                round(
                                    datasummary["count"] / len(self.data) * 100,
                                    2,
                                ),
                            )
                        ).reshape(1, 10),
                        columns=[
                            ["컬럼"] * 2 + ["연속형 대상"] * 4 + ["공통"] * 4,
                            [
                                "컬럼명",
                                "타입",
                                "최소값",
                                "최대값",
                                "평균",
                                "표준편차",
                                "NULL수",
                                "NULL%",
                                "적재건수",
                                "적재건수%",
                            ],
                        ],
                    )
                else:
                    temp_df = pd.DataFrame(
                        np.array(
                            (
                                cname,
                                ctype,
                                len(datasummary["class"]),
                                ", ".join(list(datasummary["class"].keys())),
                                str(datasummary["class"])
                                .replace("{", "")
                                .replace("}", ""),
                                datasummary["null_count"],
                                round(
                                    datasummary["null_count"] / len(self.data) * 100,
                                    2,
                                ),
                                len(self.data) - datasummary["null_count"],
                                100
                                - round(
                                    datasummary["null_count"] / len(self.data) * 100,
                                    2,
                                ),
                            )
                        ).reshape(1, 9),
                        columns=[
                            ["컬럼"] * 2 + ["범주형 대상"] * 3 + ["공통"] * 4,
                            [
                                "컬럼명",
                                "타입",
                                "범주 수",
                                "범주",
                                "범주%",
                                "NULL수",
                                "NULL%",
                                "적재건수",
                                "적재건수%",
                            ],
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