import pandas as pd
import pymysql
import os
import math
import uuid

def read_sys_office():
    office_mapping = {}
    try:
        df = pd.read_csv('sys_office.csv')
        for _, row in df.iterrows():
            name = row.get('name', '').strip()
            id_value = row.get('id', '').strip()
            if name and id_value:
                office_mapping[name] = id_value
        print(f"成功读取sys_office.csv，建立了{len(office_mapping)}个机构映射")
    except Exception as e:
        print(f"读取sys_office.csv时出错: {e}")
    return office_mapping

def process_excel(office_mapping):
    try:
        df = pd.read_excel('1.龙岩市辖区保险机构网点信息表(1).xlsx', header=1)
        
        column_mapping = {
            '机构简称': 'short_name',
            '机构全称': 'full_name',
            '联系人': 'contact',
            '手机号': 'phone'
        }
        df = df.rename(columns=column_mapping)
        
        df['office_id'] = ''
        
        for index, row in df.iterrows():
            short_name = str(row.get('short_name', '')).strip()
            if short_name in office_mapping:
                df.at[index, 'office_id'] = office_mapping[short_name]
            else:
                print(f"未找到机构简称 '{short_name}' 对应的office_id")
        
        # 使用雪花id（随机UUID，不带横杠）
        df['id'] = [str(uuid.uuid4()).replace('-', '') for _ in range(len(df))]
        
        columns = ['id', 'office_id', 'short_name', 'full_name', 'contact', 'phone']
        df = df[columns]
        
        df = df.fillna('')
        
        print(f"成功处理Excel文件，共{len(df)}条记录")
        return df
    except Exception as e:
        print(f"处理Excel文件时出错: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_connection():
    try:
        connection = pymysql.connect(
            host='localhost',
            user=;
            password;
            charset='utf8mb4'
        )
        print("成功连接到MySQL数据库")
        return connection
    except Exception as e:
        print(f"连接数据库时出错: {e}")
        return None

def create_database_and_table(connection, df):
    if connection is None:
        return False
    
    try:
        cursor = connection.cursor()
        
        cursor.execute("CREATE DATABASE IF NOT EXISTS insurance_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute("USE insurance_db")
        print("成功创建/选择数据库 insurance_db")
        
        cursor.execute("DROP TABLE IF EXISTS insurance_office_branch")
        print("已删除旧表（如果存在）")
        
        create_table_sql = """
        CREATE TABLE `insurance_office_branch` (
          `id` varchar(64) NOT NULL COMMENT '雪花ID',
          `office_id` varchar(64) DEFAULT NULL COMMENT '关联的机构ID（sys_office表）',
          `short_name` varchar(100) DEFAULT NULL COMMENT '机构简称',
          `full_name` varchar(200) DEFAULT NULL COMMENT '机构全称',
          `contact` varchar(50) DEFAULT NULL COMMENT '联系人',
          `phone` varchar(50) DEFAULT NULL COMMENT '手机号',
          PRIMARY KEY (`id`),
          KEY `idx_office_id` (`office_id`),
          KEY `idx_short_name` (`short_name`),
          KEY `idx_full_name` (`full_name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='保险机构网点信息表'
        """
        cursor.execute(create_table_sql)
        print("成功创建表 insurance_office_branch")
        
        for _, row in df.iterrows():
            insert_sql = """
            INSERT INTO insurance_office_branch
            (id, office_id, short_name, full_name, contact, phone)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = tuple(
                '' if (isinstance(v, float) and math.isnan(v)) or v == '' else str(v)
                for v in [
                    row['id'],
                    row['office_id'],
                    row['short_name'],
                    row['full_name'],
                    row['contact'],
                    row['phone']
                ]
            )
            cursor.execute(insert_sql, values)
        
        connection.commit()
        print(f"成功插入{len(df)}条记录到数据库")
        return True
    except Exception as e:
        print(f"创建表或插入数据时出错: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if connection:
            connection.close()

def save_to_excel(df):
    if df is not None:
        try:
            output_file = '处理后的保险机构网点信息表2.xlsx'
            df.to_excel(output_file, index=False)
            print(f"成功保存处理后的数据到 {output_file}")
            return output_file
        except Exception as e:
            print(f"保存Excel文件时出错: {e}")
    return None

def main():
    print("开始处理数据...")
    
    office_mapping = read_sys_office()
    df = process_excel(office_mapping)
    
    if df is not None:
        save_to_excel(df)
        
        print("\n开始保存到数据库...")
        connection = create_connection()
        if connection:
            success = create_database_and_table(connection, df)
            if success:
                print("\n=== 处理完成 ===")
                print(f"共处理 {len(df)} 条记录")
                print("数据已保存到:")
                print("1. Excel文件: 处理后的保险机构网点信息表2.xlsx")
                print("2. 数据库表: insurance_db.insurance_office_branch")
            else:
                print("保存到数据库失败")
        else:
            print("无法连接到数据库")
    else:
        print("处理失败，请检查错误信息")

if __name__ == "__main__":
    main()
