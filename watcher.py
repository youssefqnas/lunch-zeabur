# watcher.py (النسخة المعدلة)

import subprocess
import time # --- تمت الإضافة: لاستخدامه في حالة تكرار التشغيل
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException

# --- إعدادات الاتصال ---
CLICKHOUSE_HOST = "l5bxi83or6.eu-central-1.aws.clickhouse.cloud"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "8aJlVz_A2L4On"
DATABASE_NAME = 'default'

# --- إعدادات الجدول والتشغيل ---
TABLE_NAME = 'CLICKHOUSE_TABLES'
EXTERNAL_SCRIPT_NAME = 'creat_clickhouse.py'

# =================================================================
# ===   التحكم الرئيسي: حدد إجمالي عدد الصفوف المطلوب هنا   ===
# =================================================================
# هذا هو العدد الإجمالي للصفوف التي تريد أن تكون موجودة في الجدول.
# سيقوم السكربت بإضافة صفوف جديدة حتى يصل إلى هذا العدد.
DESIRED_ROW_COUNT = 100
# =================================================================


def create_table_if_not_exists(client: Client):
    """
    يتحقق من وجود الجدول، وإذا لم يكن موجودًا، يقوم بإنشائه بالهيكل المطلوب.
    (هذه الدالة لم تتغير)
    """
    try:
        print(f"Checking if table '{DATABASE_NAME}.{TABLE_NAME}' exists...")
        result = client.execute(f"EXISTS TABLE {DATABASE_NAME}.{TABLE_NAME}")
        
        if result[0][0] == 1:
            print(f"Table '{DATABASE_NAME}.{TABLE_NAME}' already exists.")
            return True

        print("\nTable does not exist. Preparing to create it...")
        fixed_columns = [
            "CLICKHOUSE_MAIL String", "CLICKHOUSE_MAIL_PASS String",
            "CLICKHOUSE_HOST String", "CLICKHOUSE_PASSWORD String"
        ]
        symbol_columns = [f"symbol{i} String" for i in range(1, 51)]
        all_columns_definitions = fixed_columns + symbol_columns
        columns_sql = ",\n  ".join(all_columns_definitions)

        create_table_query = f"""
        CREATE TABLE {DATABASE_NAME}.{TABLE_NAME} (
          {columns_sql}
        ) ENGINE = MergeTree() ORDER BY (CLICKHOUSE_MAIL)
        """
        
        print("Executing CREATE TABLE statement...")
        client.execute(create_table_query)
        print(f"\nSuccessfully created table '{DATABASE_NAME}.{TABLE_NAME}'.")
        return True

    except ServerException as e:
        print(f"An error occurred with ClickHouse server: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False


# --- تم تعديل هذه الدالة بالكامل ---
def ensure_desired_row_count(client: Client):
    """
    يتحقق من عدد الصفوف الحالية ويشغل السكربت الخارجي لإضافة الصفوف الناقصة
    حتى الوصول إلى العدد المطلوب (DESIRED_ROW_COUNT).
    """
    try:
        print("\n--- Checking row count to meet the desired target ---")
        
        # 1. الحصول على عدد الصفوف الحالية
        query = f"SELECT count() FROM {DATABASE_NAME}.{TABLE_NAME}"
        current_row_count = client.execute(query)[0][0]
        
        print(f"Current rows in table: {current_row_count}")
        print(f"Desired total rows: {DESIRED_ROW_COUNT}")
        
        # 2. حساب عدد الصفوف المطلوب إنشاؤها
        rows_to_create = DESIRED_ROW_COUNT - current_row_count
        
        # 3. اتخاذ القرار بناءً على الفارق
        if rows_to_create > 0:
            print(f"\nNeed to create {rows_to_create} new row(s). Starting the process...")
            
            # 4. تكرار عملية الإنشاء حسب عدد الصفوف الناقصة
            for i in range(rows_to_create):
                print("\n" + "="*50)
                print(f"--- Creating Row {i + 1} of {rows_to_create} ---")
                print(f"Running external script: '{EXTERNAL_SCRIPT_NAME}'...")
                
                try:
                    command_to_run = [
                        'python', 
                        EXTERNAL_SCRIPT_NAME,
                        CLICKHOUSE_HOST,
                        CLICKHOUSE_USER,
                        CLICKHOUSE_PASSWORD
                    ]
                    
                    # تشغيل السكربت وانتظار انتهائه
                    run_result = subprocess.run(command_to_run, check=True, text=True, capture_output=True)
                    print(f"Successfully executed '{EXTERNAL_SCRIPT_NAME}' for row {i + 1}.")
                    # يمكنك عرض المخرجات إذا أردت رؤية تفاصيل عملية الإنشاء
                    # print(f"Script output:\n{run_result.stdout}")
                
                except FileNotFoundError:
                    print(f"ERROR: The script '{EXTERNAL_SCRIPT_NAME}' was not found.")
                    return # إيقاف العملية إذا لم يتم العثور على السكربت
                except subprocess.CalledProcessError as e:
                    print(f"ERROR: The script '{EXTERNAL_SCRIPT_NAME}' failed to execute correctly.")
                    print(f"Return code: {e.returncode}")
                    print(f"Error Output:\n{e.stderr}")
                    print("Stopping further row creation due to an error.")
                    return # إيقاف العملية عند حدوث خطأ
                except Exception as e:
                    print(f"An unexpected error occurred while running the script: {e}")
                    return # إيقاف العملية

            print("\n" + "="*50)
            print("✅ All required rows have been created successfully!")

        else:
            print("\nThe table already has the desired number of rows (or more). No action needed.")

    except ServerException as e:
        print(f"An error occurred while counting rows: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    """
    الدالة الرئيسية للاتصال وتنفيذ المنطق
    """
    print("--- Starting ClickHouse Watcher & Manager Script ---")
    client = None
    try:
        client = Client(
            host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
            database=DATABASE_NAME, secure=True, port=9440
        )
        print("Connection to ClickHouse Cloud successful.")
        
        table_exists_or_created = create_table_if_not_exists(client)
        
        if table_exists_or_created:
            # استدعاء الدالة الجديدة
            ensure_desired_row_count(client)

    except Exception as e:
        print(f"Failed to connect to ClickHouse. Error: {e}")
    finally:
        if client:
            client.disconnect()
            print("\nConnection closed.")
        print("--- Script Finished ---")


if __name__ == "__main__":
    main()