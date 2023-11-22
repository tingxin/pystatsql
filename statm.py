import pymysql.cursors
import sys
import os
import json
import time
import threading
import hashlib
from datetime import datetime

max_count = 10

def exe_check(host_source: str, db:dict,  sql: str, cache: dict):
    db_name = db['name']
    user = db['user']
    password = db['password']
    parts = host_source.split(':')
    conn = pymysql.connect(host=parts[0],
                            port=int(parts[1]),
                            user=user,
                            password=password,
                            database=db_name,
                            connect_timeout=1000)
    
    
    with conn.cursor() as cursor:
        print(f"正在扫描 ....")
        cursor.execute(sql)
        counter = 0
        while True:
            rows = cursor.fetchmany(size=1000)  # 指定每次获取的行数
            if not rows:
                break
            for row in rows:
                # 处理每一行数据
                row_str = str(row)
                # # 使用 hashlib 计算哈希值
                # sha256_hash = hashlib.sha256(row_str.encode()).hexdigest()
                cache[row_str] = True
                if counter > max_count:
                    break
                
                counter +=1
                if counter % 10000 == 0:
                    print(f"正在扫描 {counter} 行....")

    conn.close()
    


    


def compare(task_fix,sql, result1: dict, result2: dict):

    l1 = len(result1)
    l2 = len(result2)
    big_name = '集群1'
    small_name = '集群2'
    big = result1
    small = result2
    if l1 < l2:
        big, small = result2, result1
        big_name,small_name = small_name, big_name

    check = dict()
    for key in big:
        if key not in small:
            check[key] = f"not in {small_name}"
        else:
            if big[key] != small[key]:
                check[key] = f"{key} not match: \n{big[key]}\n{small[key]}"

    with open(f'{os.getcwd()}/compare_mysql_{task_fix}.txt', 'a') as file:
        file.write(f'SQL\n: {sql}\n分析结果:\n')
        file.write(f'在集群1 扫描了: {l1}\n')
        file.write(f'在集群2 扫描了: {l2}\n')
        if l1 == l2:
            file.write('行数一致\n')
        else:
            file.write('错误：行数不致\n')
        if len(check) > 0:
            file.write('错误：')
            for key, value in check.items():
                file.write(f'{key}: {value}\n')
        else:
            file.write(f'数据全部一致\n')
        file.write(f'\t\n')
        
    del check


def run(task_fix, host_source, host_target, db1,db2,sql):
    # 创建两个线程
    cache1 = dict()
    cache2 = dict()
    thread1 = threading.Thread(target=exe_check, args=(host_source, db1,sql,cache1,))
    thread2 = threading.Thread(target=exe_check, args=(host_target, db2,sql,cache2,))

    # 启动线程
    print(f"begin scan between {host_source} and {host_target} with sql:\n{sql}")
    thread1.start()
    thread2.start()

    # 等待两个线程执行完毕
    thread1.join()
    thread2.join()


    print(f"begin compare  between {host_source} and {host_target} with sql:\n{sql}")
    compare(task_fix, sql, cache1,cache2)
    print(f"end scan compare between {host_source} and {host_target} with sql:\n{sql}")

    del cache1
    del cache2


def main():
    arguments = sys.argv
    conf_path = f"{os.getcwd()}/{arguments[1]}"
    print(conf_path)
    # 读取 JSON 文件
    with open(conf_path, 'r') as file:
        # 加载 JSON 数据
        conf = json.load(file)

    host_source = conf['host_source']
    host_target = conf['host_target']
    global max_count
    max_count = conf['max_count']


    current_time = datetime.now()
    # 将当前时间格式化为字符串
    task_fix = current_time.strftime("%Y-%m-%d_%H_%M_%S")
    for item in conf['dbs']:
        
        db1 = item['db1']
        db2 = item['db2']
        print(f"开始处理数据库 {db1['name']}和{db2['name']}")
       
        tables = item['tables']

        for table in tables:
            start_time = time.time()
            sql = table['sql']

            run(task_fix, host_source, host_target, db1,db2, sql)
            
            end_time = time.time()

            # 计算程序运行时间
            elapsed_time = end_time - start_time
            print(f"校验表:\n{sql} 耗时: {elapsed_time:.2f} 秒")


main()
