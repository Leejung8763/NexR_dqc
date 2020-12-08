import sys

sys.path.append('../')

from PreProcess import PreProcess 

input_path = './cab_rides.csv'
output_path = './output' 

test = PreProcess(input_path)

test.eda()
test.dqc()

test.save(output_path)


