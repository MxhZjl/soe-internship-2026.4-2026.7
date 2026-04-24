import pandas as pd
import pymysql
import uuid
from datetime import datetime

# 读取sys_office.csv文件，获取机构ID映射
def load_office_mapping():
    df_office = pd.read_csv('sys_office.csv', encoding='utf-8')
    # 创建机构名称到ID的映射
    office_mapping = {}
    for index, row in df_office.iterrows():
        name = row.get('name', '').strip()
        id_val = row.get('id', '').strip()
        if name and id_val:
            office_mapping[name] = id_val
    return office_mapping

# 读取Excel文件并处理

def process_first_excel(file_path, office_mapping):
    """处理第一个Excel文件"""
    insert_statements = []
    try:
        # 读取Excel文件，跳过第一行标题，使用第二行作为列名
        df = pd.read_excel(file_path, header=1)
        
        # 遍历数据行
        for index, row in df.iterrows():
            # 不需要生成ID，使用AUTO_INCREMENT
            
            # 获取数据
            short_name = str(row.iloc[1]).strip() if len(row) > 1 else ''
            full_name = str(row.iloc[2]).strip() if len(row) > 2 else ''
            
            # 匹配office_id
            office_id = office_mapping.get(short_name, '')
            
            # 其他字段
            county = ''
            address = ''
            fixed_phone = ''
            customer_service_phone = ''
            
            # 时间戳
            create_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            update_time = create_time
            del_flag = '0'
            
            # 生成插入语句
            if short_name or full_name:
                insert_sql = f"""INSERT INTO `insurance_office_branch` 
                (`office_id`, `short_name`, `full_name`, `county`, `address`, 
                `fixed_phone`, `customer_service_phone`, `create_time`, `update_time`, `del_flag`) 
                VALUES (
                    '{office_id}',
                    '{short_name}',
                    '{full_name}',
                    '{county}',
                    '{address}',
                    '{fixed_phone}',
                    '{customer_service_phone}',
                    '{create_time}',
                    '{update_time}',
                    '{del_flag}'
                )"""
                insert_statements.append(insert_sql)
    except Exception as e:
        print(f"处理文件 {file_path} 失败: {e}")
    return insert_statements

def process_second_excel(file_path, office_mapping):
    """处理第二个Excel文件"""
    insert_statements = []
    try:
        # 读取Excel文件，跳过第一行标题，使用第二行作为列名
        df = pd.read_excel(file_path, header=1)
        
        # 遍历数据行
        for index, row in df.iterrows():
            # 不需要生成ID，使用AUTO_INCREMENT
            
            # 获取数据
            try:
                short_name = str(row[0]).strip() if pd.notna(row[0]) else ''
                full_name = str(row[1]).strip() if pd.notna(row[1]) else ''
                county = str(row[2]).strip() if pd.notna(row[2]) else ''
                address = str(row[3]).strip() if pd.notna(row[3]) else ''
                fixed_phone = str(row[4]).strip() if pd.notna(row[4]) else ''
                customer_service_phone = str(row[5]).strip() if pd.notna(row[5]) else ''
            except:
                # 如果按索引访问失败，尝试按列名访问
                short_name = str(row.get('机构简称', '')).strip()
                full_name = str(row.get('机构全称', '')).strip()
                county = str(row.get('县域', '')).strip()
                address = str(row.get('保险机构线下查询网点地址', '')).strip()
                fixed_phone = str(row.get('固定电话', '')).strip()
                customer_service_phone = str(row.get('机构客服热线', '')).strip()
            
            # 匹配office_id
            office_id = office_mapping.get(short_name, '')
            
            # 时间戳
            create_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            update_time = create_time
            del_flag = '0'
            
            # 生成插入语句
            if short_name or full_name:
                insert_sql = f"""INSERT INTO `insurance_office_branch` 
                (`office_id`, `short_name`, `full_name`, `county`, `address`, 
                `fixed_phone`, `customer_service_phone`, `create_time`, `update_time`, `del_flag`) 
                VALUES (
                    '{office_id}',
                    '{short_name}',
                    '{full_name}',
                    '{county}',
                    '{address}',
                    '{fixed_phone}',
                    '{customer_service_phone}',
                    '{create_time}',
                    '{update_time}',
                    '{del_flag}'
                )"""
                insert_statements.append(insert_sql)
    except Exception as e:
        print(f"处理文件 {file_path} 失败: {e}")
    return insert_statements

# 处理数据并生成SQL插入语句
def process_data(office_mapping):
    # 只处理第二个Excel文件，因为它包含详细信息
    statements = process_second_excel('2.龙岩市辖区保险机构网点信息表（含县域）（4.15）(1).xlsx', office_mapping)
    
    return statements

# 执行SQL语句
def execute_sql_statements(statements):
    try:
        # 先连接到MySQL服务器（不指定数据库）
        conn = pymysql.connect(
            host='localhost',
            user=
            password=
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        
        # 创建数据库（如果不存在）
        db_name = 'insurance_db'
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print(f"数据库 {db_name} 创建成功")
        
        # 选择数据库
        cursor.execute(f"USE {db_name}")
        
        # 创建表
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS `insurance_office_branch` (
            `id` int NOT NULL AUTO_INCREMENT COMMENT '主键ID',
            `office_id` varchar(64) DEFAULT NULL COMMENT '关联的机构ID（sys_office表）',
            `short_name` varchar(100) DEFAULT NULL COMMENT '机构简称',
            `full_name` varchar(200) DEFAULT NULL COMMENT '机构全称',
            `county` varchar(100) DEFAULT NULL COMMENT '县域',
            `address` varchar(500) DEFAULT NULL COMMENT '保险机构线下查询网点地址',
            `fixed_phone` varchar(50) DEFAULT NULL COMMENT '固定电话',
            `customer_service_phone` varchar(50) DEFAULT NULL COMMENT '机构客服热线',
            `create_time` datetime DEFAULT NULL COMMENT '创建时间',
            `update_time` datetime DEFAULT NULL COMMENT '更新时间',
            `del_flag` char(1) DEFAULT '0' COMMENT '删除标记：0-未删除，1-已删除',
            PRIMARY KEY (`id`),
            KEY `idx_office_id` (`office_id`),
            KEY `idx_short_name` (`short_name`),
            KEY `idx_full_name` (`full_name`),
            KEY `idx_county` (`county`),
            KEY `idx_del_flag` (`del_flag`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='保险机构网点信息表';
        """
        cursor.execute(create_table_sql)
        print("表创建成功")
        
        # 清空表数据
        cursor.execute("TRUNCATE TABLE insurance_office_branch")
        print("表数据清空成功")
        
        # 执行插入语句
        for sql in statements:
            try:
                cursor.execute(sql)
            except Exception as e:
                print(f"执行SQL失败: {e}")
                print(f"SQL语句: {sql}")
        
        # 提交事务
        conn.commit()
        print(f"成功插入 {len(statements)} 条记录")
        
    except Exception as e:
        print(f"数据库操作失败: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # 加载机构映射
    office_mapping = load_office_mapping()
    print(f"加载了 {len(office_mapping)} 个机构映射")
    
    # 处理数据
    insert_statements = process_data(office_mapping)
    print(f"生成了 {len(insert_statements)} 条插入语句")
    
    # 执行SQL
    execute_sql_statements(insert_statements)
