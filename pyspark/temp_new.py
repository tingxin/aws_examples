from datetime import date, timedelta

begin = date(year=2021, month=10, day=1)
end = date(year=2021, month=10, day=10)
file_path_list = list()
file_path_f = "s3://mcc-operation-logs/operation-log-{}/"

current = begin
while current <= end:
    file_path_list.append(file_path_f.format(current))
    current = current + timedelta(days=1)

print(file_path_list)

