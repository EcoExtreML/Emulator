import argparse

# 设置命令行参数
parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, required=True, help='Year for the data')
parser.add_argument('--month', type=int, required=True, help='Month for the data')

args = parser.parse_args()

year = args.year
month = args.month

# 使用 year 和 month 执行你的逻辑
print(f"Processing data for {year}-{month:02d}")
