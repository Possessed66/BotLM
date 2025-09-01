# plot_generator.py
import logging
import pandas as pd
import matplotlib.pyplot as plt
import os
import tempfile
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from Config_plot import SERVICE_ACCOUNT_INFO, SPREADSHEET_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_sheets_service():
    """Получить сервис Google Sheets"""
    
    
    creds = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO,  # <-- Используем from_service_account_info
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds)

def read_sheet(sheets_service, spreadsheet_id, sheet_name):
    """Прочитать лист Google Sheets"""
    try:
        # Сначала получаем список всех листов
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])
        sheet_names = [sheet['properties']['title'] for sheet in sheets]
        
        # Проверяем, существует ли лист
        if sheet_name not in sheet_names:
            available_sheets = ', '.join(sheet_names[:10])  # Показываем первые 10
            raise ValueError(f"Лист '{sheet_name}' не найден. Доступные листы: {available_sheets}")
        
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A1:ZZ'
        ).execute()
        values = result.get('values', [])
        if not values:
            raise ValueError(f"Лист '{sheet_name}' пуст")
        return pd.DataFrame(values)
    except Exception as e:
        raise Exception(f"Ошибка чтения листа '{sheet_name}': {str(e)}")

def extract_dates_from_headers(df):
    """Извлечь пары дат из заголовков"""
    dates = []
    col_idx = 3  # Начинаем с столбца D
    
    while col_idx + 1 < len(df.columns):
        if col_idx < len(df.columns) and col_idx + 1 < len(df.columns):
            start_date_str = str(df.iloc[0, col_idx]).strip() if pd.notna(df.iloc[0, col_idx]) else None
            end_date_str = str(df.iloc[0, col_idx + 1]).strip() if pd.notna(df.iloc[0, col_idx + 1]) else None
            
            if start_date_str and end_date_str:
                try:
                    start_date = datetime.strptime(start_date_str, "%d.%m")
                    end_date = datetime.strptime(end_date_str, "%d.%m")
                    dates.append((start_date, end_date))
                except ValueError:
                    pass  # Пропускаем некорректные даты
        
        col_idx += 2
    
    return dates

def select_period(dates, start_str, end_str):
    """Выбрать даты в заданном периоде"""
    try:
        start_date = datetime.strptime(start_str, "%d.%m")
        end_date = datetime.strptime(end_str, "%d.%m")
    except ValueError:
        raise Exception("Неверный формат даты. Используйте dd.mm")
    
    selected = []
    for d_start, d_end in dates:
        if d_start <= end_date and d_end >= start_date:
            selected.append((d_start, d_end))
    
    return selected

def find_date_column(df, date_str):
    """Найти индекс столбца по дате"""
    if len(df) == 0 or len(df.iloc[0]) == 0:
        return None
    for col_idx, col_name in enumerate(df.iloc[0]):
        if str(col_name).strip() == date_str:
            return col_idx
    return None

def parse_department_sheet(df, selected_dates=None):
    """Парсить данные листа отдела"""
    data = {}
    current_indicator = None
    row_idx = 0
    
    while row_idx < len(df):
        if len(df.iloc[row_idx]) == 0:
            row_idx += 1
            continue
            
        row = df.iloc[row_idx]
        
        # Проверяем, является ли строка заголовком показателя
        if pd.notna(row[0]) and str(row[0]).strip() and str(row[0]).strip() != '':
            if row_idx > 0:  # Не для первой строки
                current_indicator = str(row[0]).strip()
                logger.info(f"Обнаружен показатель: {current_indicator}")
                data[current_indicator] = {}
                row_idx += 1
                continue
        
        # Если текущий показатель не определен, переходим к следующей строке
        if current_indicator is None:
            row_idx += 1
            continue
        
        # Проверяем, есть ли данные в строке
        if len(row) <= 2 or pd.isna(row[2]):
            row_idx += 1
            continue
        
        try:
            shop = int(row[2])
        except (ValueError, IndexError):
            row_idx += 1
            continue
        
        values = []
        ratings = []
        
        if selected_dates:
            for start_date, end_date in selected_dates:
                date_str = start_date.strftime("%d.%m")
                col_idx = find_date_column(df, date_str)
                
                if col_idx is not None and col_idx < len(row):
                    # Значение показателя
                    value = None
                    if col_idx < len(row) and pd.notna(row[col_idx]):
                        try:
                            value_str = str(row[col_idx])
                            logger.debug(f"Исходное значение: {repr(value_str)}")
                            
                            value_str = value_str.strip().replace('\xa0', '').replace(' ', '')
                            value_str = value_str.replace(',', '.')
                            
                            logger.debug(f"Очищенное значение: {repr(value_str)}")
                            if value_str and value_str != '':
                                value = float(value_str)
                        except ValueError:
                            pass
                    
                    # Рейтинг
                    rating = None
                    if col_idx + 1 < len(row) and pd.notna(row[col_idx + 1]):
                        try:
                            rating_str = str(row[col_idx + 1])
                            if rating_str.strip():
                                rating = int(float(rating_str))
                        except ValueError:
                            pass
                    
                    values.append(value)
                    ratings.append(rating)
                else:
                    values.append(None)
                    ratings.append(None)
        else:
            # Если даты не выбраны, парсим все подряд
            col_idx = 3
            while col_idx + 1 < len(row):
                # Значение
                value = None
                if col_idx < len(row) and pd.notna(row[col_idx]):
                    try:
                        value_str = str(row[col_idx]).replace(',', '.')
                        if value_str.strip():
                            value = float(value_str)
                    except ValueError:
                        pass
                
                # Рейтинг
                rating = None
                if col_idx + 1 < len(row) and pd.notna(row[col_idx + 1]):
                    try:
                        rating_str = str(row[col_idx + 1])
                        if rating_str.strip():
                            rating = int(float(rating_str))
                    except ValueError:
                        pass
                
                values.append(value)
                ratings.append(rating)
                col_idx += 2
        
        if current_indicator not in data:
            data[current_indicator] = {}
        data[current_indicator][shop] = {'values': values, 'ratings': ratings}
        row_idx += 1
    
    logger.info(f"Результат парсинга: {list(data.keys())}")
    return data

def plot_indicator_trend(indicator_data, indicator_name, output_path, dates=None):
    """Построить график динамики показателя"""
    if dates is None:
        dates = ['30.12–05.01', '06.01–12.01']
    
    # Проверяем, есть ли данные для построения
    has_values_data = any(
        any(v is not None for v in shop_data['values']) 
        for shop_data in indicator_data.values()
    )
    has_ratings_data = any(
        any(v is not None for v in shop_data['ratings']) 
        for shop_data in indicator_data.values()
    )
    
    if not (has_values_data or has_ratings_data):
        return  # Нет данных для графика
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle(f"Динамика показателя: {indicator_name}", fontsize=14, fontweight='bold')
    
    # График значений
    if has_values_data:
        for shop, data in indicator_data.items():
            if any(v is not None for v in data['values']):
                ax1.plot(dates, data['values'], marker='o', linewidth=2, markersize=6, label=f"Магазин {shop}")
        ax1.set_title("Значения показателя", fontsize=12)
        ax1.set_ylabel("Значение", fontsize=10)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
    else:
        ax1.set_visible(False)
    
    # График рейтингов
    if has_ratings_data:
        for shop, data in indicator_data.items():
            if any(v is not None for v in data['ratings']):
                ax2.plot(dates, data['ratings'], marker='s', linewidth=2, markersize=6, label=f"Магазин {shop}")
        ax2.set_title("Рейтинг (позиция)", fontsize=12)
        ax2.set_ylabel("Позиция", fontsize=10)
        ax2.set_xlabel("Период", fontsize=10)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
    else:
        ax2.set_visible(False)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

def generate_plots_for_department(dept_name, start_date_str, end_date_str):
    """
    Основная функция: генерирует графики и возвращает список путей к файлам
    """
    try:
        service = get_sheets_service()
        df_dept = read_sheet(service, SPREADSHEET_ID, dept_name)
        
        if df_dept.empty:
            raise Exception(f"Лист '{dept_name}' пуст или не существует")
        
        all_dates = extract_dates_from_headers(df_dept)
        selected_dates = select_period(all_dates, start_date_str, end_date_str)
        
        if not selected_dates:
            raise Exception("Не найдено данных за указанный период")
        
        parsed_data = parse_department_sheet(df_dept, selected_dates)
        
        logger.info(f"Найдено показателей: {len(parsed_data)}")
        
        # Создаем временные файлы
        temp_files = []
        temp_dir = tempfile.mkdtemp()
        
        for indicator, shops_data in parsed_data.items():
            # Проверяем, есть ли данные для этого показателя
            has_data = any(
                any(v is not None for v in shop_data['values']) or 
                any(v is not None for v in shop_data['ratings'])
                for shop_data in shops_data.values()
            )
            
            logger.info(f"Показатель '{indicator}': has_data = {has_data}")
            
            if has_data:
                safe_indicator_name = "".join(c for c in indicator if c.isalnum() or c in (' ', '-', '_')).rstrip()
                output_path = os.path.join(temp_dir, f"{dept_name}_{safe_indicator_name}.png")
                date_labels = [f"{d[0].strftime('%d.%m')}-{d[1].strftime('%d.%m')}" for d in selected_dates]
                plot_indicator_trend(shops_data, indicator, output_path, date_labels)
                temp_files.append(output_path)
                logger.info(f"График создан для '{indicator}'")
            else:
                logger.info(f"Показатель '{indicator}' не имеет данных для построения графика")
                # Дополнительная проверка: выводим данные для диагностики
                logger.info(f"Данные для '{indicator}': {shops_data}")
                
        return temp_files
        
    except Exception as e:
        raise Exception(f"Ошибка при генерации графиков: {str(e)}")
        
    except Exception as e:
        raise Exception(f"Ошибка при генерации графиков: {str(e)}")
