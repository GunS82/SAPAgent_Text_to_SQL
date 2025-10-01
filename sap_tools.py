from pysapscript import Sapscript, exceptions
import win32clipboard
import time
import json
import re
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.CRITICAL)

import logging
import time
import win32clipboard
from pysapscript import Sapscript, exceptions

def run_sap_sql_query(sql_query: str) -> dict:
    """
    Выполняет SQL-запрос в SAP и возвращает результат выполнения, статус и сообщение.
    """
    sap = Sapscript()
    win = sap.attach_window(0, 0)
    
    try:
        win.maximize()
        logging.info("Setting SQL query text...")
        query_element_id = "wnd[0]/usr/tabsSQL/tabpINPUT/ssubINPUT_REF1:SAPLSHDBCCMS:0109/cntlSQL_INPUT_CONT/shellcont/shell"
        query_shell = win.session_handle.findById(query_element_id)
        query_shell.text = sql_query
        query_shell.setSelectionIndexes(152, 152)
        
        logging.info("Executing SQL query...")
        win.press("wnd[0]/tbar[1]/btn[8]")
        
        # Check ALV table for success or error
        #alv_table_id = "/app/con[0]/ses[0]/wnd[0]/shellcont[0]/shell"
        alv_table_id = "wnd[0]/shellcont[0]/shell"
        alv_table = win.read_shell_table(alv_table_id)
        icon_value = alv_table.cell(0, "ICON")
        
        if icon_value.startswith("@8O\\Q"):  # Проверка на наличие иконки ошибки
            error_message = alv_table.cell(0, "MESSAGE")
            return {"status": False, "message": error_message, "result": "Ошибка выполнения"}
        elif icon_value.startswith("@5B\\Q"):  # Проверка на успех выполнения
            success_message = alv_table.cell(0, "MESSAGE")
            logging.info("Query executed successfully, proceeding to export results...")

            logging.info("Exporting to clipboard...")
            output_shell_id = "wnd[0]/usr/tabsSQL/tabpOUTPUT/ssubOUTPUT_REF1:SAPLSHDBCCMS:0110/cntlSQL_OUTPUT_CONT/shellcont/shell"
            output_shell = win.session_handle.findById(output_shell_id)
            output_shell.pressToolbarContextButton("&MB_EXPORT")
            output_shell.selectContextMenuItem("&PC")
            
            format_option_id = "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[4,0]"
            format_option = win.session_handle.findById(format_option_id)
            format_option.select()
            format_option.setFocus()
            win.press("wnd[1]/tbar[0]/btn[0]")
            
            time.sleep(1)  # Wait for the clipboard to get data
            win32clipboard.OpenClipboard()
            try:
                clipboard_data = win32clipboard.GetClipboardData()
                logging.debug(f"Clipboard data:\n{clipboard_data}")
            finally:
                win32clipboard.CloseClipboard()
            
            if not clipboard_data:
                return {"status": False, "message": "Данные не найдены", "result": "Данные не найдены"}
            
            return {"status": True, "message": success_message, "result": clipboard_data}
        
        else:
            return {"status": False, "message": "Неизвестный статус выполнения запроса", "result": "Ошибка выполнения"}
    
    except exceptions.ActionException as e:
        logging.error("SAP GUI action failed.")
        sap.handle_exception_with_screenshot(e)
        return {"status": False, "message": str(e), "result": "Ошибка выполнения"}
    
    except Exception as e:
        logging.error("Unexpected error occurred.", exc_info=True)
        sap.handle_exception_with_screenshot(e, "general_error")
        return {"status": False, "message": str(e), "result": "Ошибка выполнения"}




def get_table_fields(table_name: str, lang: str = "ru") -> str:
    """
    Извлекает поля указанной таблицы SAP с ключевыми характеристиками на русском языке
    и возвращает результат в виде JSON-строки.
    """
    sap = Sapscript()
    win = sap.attach_window(0, 0)

    try:
        win.maximize()

        query = f"""
        SELECT FIELDNAME, FLDSTAT, KEYFLAG, DOMNAME, CHECKTABLE, DATATYPE, OUTPUTLEN, DECIMALS, LOWERCASE, DDTEXT
        FROM DD03M WHERE DDLANGUAGE = 'R' AND TABNAME = '{table_name}'
        """
        
        logging.info(f"Executing query for table: {table_name}")
        
        query_element_id = "wnd[0]/usr/tabsSQL/tabpINPUT/ssubINPUT_REF1:SAPLSHDBCCMS:0109/cntlSQL_INPUT_CONT/shellcont/shell"
        query_shell = win.session_handle.findById(query_element_id)
        query_shell.text = query
        query_shell.setSelectionIndexes(152, 152)

        logging.debug("Running the query.")
        win.press("wnd[0]/tbar[1]/btn[8]")

        logging.debug("Exporting data to clipboard.")
        output_shell_id = "wnd[0]/usr/tabsSQL/tabpOUTPUT/ssubOUTPUT_REF1:SAPLSHDBCCMS:0110/cntlSQL_OUTPUT_CONT/shellcont/shell"
        output_shell = win.session_handle.findById(output_shell_id)
        output_shell.pressToolbarContextButton("&MB_EXPORT")
        output_shell.selectContextMenuItem("&PC")

        logging.debug("Confirming export format.")
        format_option_id = "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[4,0]"
        format_option = win.session_handle.findById(format_option_id)
        format_option.select()
        format_option.setFocus()
        win.press("wnd[1]/tbar[0]/btn[0]")

        time.sleep(1)  # Wait for the data to be copied to the clipboard

        win32clipboard.OpenClipboard()
        try:
            clipboard_data = win32clipboard.GetClipboardData()
            logging.debug(f"Clipboard data:\n{clipboard_data}")
        finally:
            win32clipboard.CloseClipboard()

        if "FIELDNAME" not in clipboard_data:
            logging.warning("No data found for the provided table.")
            return "{}"
        else:
            return clipboard_data


    except exceptions.ActionException as e:
        logging.error("SAP GUI action failed.")
        sap.handle_exception_with_screenshot(e)
        return "{}"
    except Exception as e:
        logging.error("Unexpected error occurred.", exc_info=True)
        sap.handle_exception_with_screenshot(e, "general_error")
        return "{}"
    
def are_tables_present_v2(table_names: list) -> dict:
    """
    Проверяет наличие текстов таблиц в DD02T для набора имен.
    Возвращает {TABNAME: bool}, где True, если есть запись в DD02T
    для DDLANGUAGE IN ('R','E'). Если нужен именно русский — см. флаг only_ru.
    """
    if not table_names:
        return {}

    # Убираем дубликаты и приводим к верхнему регистру (имена таблиц в DDIC — upper)
    tabs = sorted({str(t).strip().upper() for t in table_names if str(t).strip()})
    # Безопасно формируем IN ('A','B',...)
    in_list = ",".join("'" + t.replace("'", "''") + "'" for t in tabs)

    # Агрегируем по TABNAME и вытаскиваем флаги наличия рус/англ
    sql = f"""
    SELECT TABNAME,
           COUNT(*) AS CNT,
           MAX(CASE WHEN DDLANGUAGE = 'R' THEN 1 ELSE 0 END) AS HAS_R,
           MAX(CASE WHEN DDLANGUAGE = 'E' THEN 1 ELSE 0 END) AS HAS_E
    FROM DD02T
    WHERE DDLANGUAGE IN ('R','E')
      AND TABNAME IN ({in_list})
    GROUP BY TABNAME
    """

    # Выполняем через существующую обвязку и парсим буфер как таблицу с '|' детерминированно
    exec_res = run_sap_sql_query(sql)
    if not exec_res.get("status"):
        return {t: False for t in tabs}

    clipboard_data = exec_res.get("result", "")
    # Пример формата:
    # |TABNAME|CNT|HAS_R|HAS_E|
    # |MARA   | 1 |  0  |  1  |
    results = {t: False for t in tabs}

    for line in clipboard_data.splitlines():
        if "|" not in line or set(line.strip()) <= {"-", " "}:
            continue
        # Разбиваем по '|' и чистим
        parts = [c.strip() for c in line.split("|")]
        # Ожидаем как минимум 5 столбцов: '', TABNAME, CNT, HAS_R, HAS_E, ''
        # Но перестрахуемся и найдём TABNAME по названию заголовка/позиции
        if len(parts) < 3 or parts[1] in ("TABNAME", ""):
            continue
        tab = parts[1].upper()
        if tab not in results:
            continue
        # Попытка чтения CNT/HAS_R/HAS_E
        try:
            cnt = int(parts[2])
        except ValueError:
            cnt = 0
        # Правило: считаем таблицу найденной, если cnt > 0
        results[tab] = cnt > 0

    return results


def are_tables_present(table_names: list) -> dict:
    """
    Проверяет наличие текстов таблиц в DD02T для набора имен.
    Возвращает {TABNAME: bool}, где True, если есть запись в DD02T
    для DDLANGUAGE IN ('R','E'). Если нужен именно русский — см. флаг only_ru.
    """
    if not table_names:
        return {}

    # Убираем дубликаты и приводим к верхнему регистру (имена таблиц в DDIC — upper)
    tabs = sorted({str(t).strip().upper() for t in table_names if str(t).strip()})
    # Безопасно формируем IN ('A','B',...)
    in_list = ",".join("'" + t.replace("'", "''") + "'" for t in tabs)

    # Агрегируем по TABNAME и вытаскиваем флаги наличия рус/англ
    sql = f"""
    SELECT TABNAME,
           COUNT(*) AS CNT,
           MAX(CASE WHEN DDLANGUAGE = 'R' THEN 1 ELSE 0 END) AS HAS_R,
           MAX(CASE WHEN DDLANGUAGE = 'E' THEN 1 ELSE 0 END) AS HAS_E
    FROM DD02T
    WHERE DDLANGUAGE IN ('R','E')
      AND TABNAME IN ({in_list})
    GROUP BY TABNAME
    """

    # Выполняем через существующую обвязку и парсим буфер как таблицу с '|' детерминированно
    exec_res = run_sap_sql_query(sql)
    if not exec_res.get("status"):
        return {t: False for t in tabs}

    clipboard_data = exec_res.get("result", "")
    # Пример формата:
    # |TABNAME|CNT|HAS_R|HAS_E|
    # |MARA   | 1 |  0  |  1  |
    results = {t: False for t in tabs}

    for line in clipboard_data.splitlines():
        if "|" not in line or set(line.strip()) <= {"-", " "}:
            continue
        # Разбиваем по '|' и чистим
        parts = [c.strip() for c in line.split("|")]
        # Ожидаем как минимум 5 столбцов: '', TABNAME, CNT, HAS_R, HAS_E, ''
        # Но перестрахуемся и найдём TABNAME по названию заголовка/позиции
        if len(parts) < 3 or parts[1] in ("TABNAME", ""):
            continue
        tab = parts[1].upper()
        if tab not in results:
            continue
        # Попытка чтения CNT/HAS_R/HAS_E
        try:
            cnt = int(parts[2])
        except ValueError:
            cnt = 0
        # Правило: считаем таблицу найденной, если cnt > 0
        results[tab] = cnt > 0

    return results


def get_domain_texts (domain_name: str, lang: str = "R") -> str:
    """
    Извлекает текстовые значения поля указанного домена (по умолчанию на русском языке)
    и возвращает результат в виде строки.
    """
    sap = Sapscript()
    win = sap.attach_window(0, 0)

    try:
        win.maximize()

        query = f"""
        SELECT VALPOS, DOMVALUE_L, DOMVALUE_H, DDTEXT
        FROM DD07V WHERE DDLANGUAGE = '{lang}' AND DOMNAME = '{domain_name}'
        """
        
        logging.info(f"Executing query for table: {domain_name}")
        
        query_element_id = "wnd[0]/usr/tabsSQL/tabpINPUT/ssubINPUT_REF1:SAPLSHDBCCMS:0109/cntlSQL_INPUT_CONT/shellcont/shell"
        query_shell = win.session_handle.findById(query_element_id)
        query_shell.text = query
        query_shell.setSelectionIndexes(152, 152)

        logging.debug("Running the query.")
        win.press("wnd[0]/tbar[1]/btn[8]")

        logging.debug("Exporting data to clipboard.")
        output_shell_id = "wnd[0]/usr/tabsSQL/tabpOUTPUT/ssubOUTPUT_REF1:SAPLSHDBCCMS:0110/cntlSQL_OUTPUT_CONT/shellcont/shell"
        output_shell = win.session_handle.findById(output_shell_id)
        output_shell.pressToolbarContextButton("&MB_EXPORT")
        output_shell.selectContextMenuItem("&PC")

        logging.debug("Confirming export format.")
        format_option_id = "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[4,0]"
        format_option = win.session_handle.findById(format_option_id)
        format_option.select()
        format_option.setFocus()
        win.press("wnd[1]/tbar[0]/btn[0]")

        time.sleep(1)  # Wait for the data to be copied to the clipboard

        win32clipboard.OpenClipboard()
        try:
            clipboard_data = win32clipboard.GetClipboardData()
            logging.debug(f"Clipboard data:\n{clipboard_data}")
        finally:
            win32clipboard.CloseClipboard()

        if "VALPOS" not in clipboard_data:
            logging.warning("No data found for the provided domain.")
            return "{}"
        else:
            return clipboard_data


    except exceptions.ActionException as e:
        logging.error("SAP GUI action failed.")
        sap.handle_exception_with_screenshot(e)
        return "{}"
    except Exception as e:
        logging.error("Unexpected error occurred.", exc_info=True)
        sap.handle_exception_with_screenshot(e, "general_error")
        return "{}"