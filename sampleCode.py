import sys
from PreProcess import PreProcess 

inputData = './editTest/LOGIN_ID_MGT.csv'
docsPath = './editTest/documents'
outputPath = '.' 

code = PreProcess(inputData, docsPath)

code.summary()
code.eda()
code.save(outputPath)