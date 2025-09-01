# plot_generator.py
import pandas as pd
import matplotlib.pyplot as plt
import os
import tempfile
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from Config_plot import SERVICE_ACCOUNT_FILE, SPREADSHEET_ID

def get_sheets_service():
    """Получить сервис Google Sheets"""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, 
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds)

def read_sheet(sheets_service, spreadsheet_id, sheet_name):
    """Прочитать лист Google Sheets"""
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A1:ZZ'
        ).execute()
        values = result.get('values', [])
        if not values:
            raise ValueError(f"No data found in sheet: {sheet_name}")
        return pd.DataFrame(values)
    except Exception as e:
        raise Exception(f"Ошибка чтения листа '{sheet_name}': {str(e)}")

def extract_dates_from_headers(df):
    """Извлечь пары дат из заголовков"""
    dates = []
    col_idx = 3  # Начинаем с столбца D
    
    while col_idx + 1 < len(df.columns):
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
        row = df.iloc[row_idx]
        
        if pd.notna(row[0]) and str(row[0]).strip() and row_idx > 0:
            current_indicator = str(row[0]).strip()
            data[current_indicator] = {}
            row_idx += 1
            continue
        
        if current_indicator is None:
            row_idx += 1
            continue
        
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
                
                if col_idx is not None:
                    value = None
                    if col_idx < len(row) and pd.notna(row[col_idx]):
                        try:
                            value = float(str(row[col_idx]).replace(',', '.'))
                        except ValueError:
                            pass
                    
                    rating = None
                    if col_idx + 1 < len(row) and pd.notna(row[col_idx + 1]):
                        try:
                            rating = int(row[col_idx + 1])
                        except ValueError:
                            pass
                    
                    values.append(value)
                    ratings.append(rating)
                else:
                    values.append(None)
                    ratings.append(None)
        else:
            col_idx = 3
            while col_idx + 1 < len(row):
                value = None
                if pd.notna(row[col_idx]):
                    try:
                        value = float(str(row[col_idx]).replace(',', '.'))
                    except ValueError:
                        pass
                
                rating = None
                if pd.notna(row[col_idx + 1]):
                    try:
                        rating = int(row[col_idx + 1])
                    except ValueError:
                        pass
                
                values.append(value)
                ratings.append(rating)
                col_idx += 2
        
        data[current_indicator][shop] = {'values': values, 'ratings': ratings}
        row_idx += 1
    
    return data

def plot_indicator_trend(indicator_data, indicator_name, output_path, dates=None):
    """Построить график динамики показателя"""
    if dates is None:
        dates = ['30.12–05.01', '06.01–12.01']
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle(f"Динамика показателя: {indicator_name}", fontsize=14, fontweight='bold')
    
    for shop, data in indicator_data.items():
        ax1.plot(dates, data['values'], marker='o', linewidth=2, markersize=6, label=f"Магазин {shop}")
    
    ax1.set_title("Значения показателя", fontsize=12)
    ax1.set_ylabel("Значение", fontsize=10)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    for shop, data in indicator_data.items():
        ax2.plot(dates, data['ratings'], marker='s', linewidth=2, markersize=6, label=f"Магазин {shop}")
    
    ax2.set_title("Рейтинг (позиция)", fontsize=12)
    ax2.set_ylabel("Позиция", fontsize=10)
    ax2.set_xlabel("Период", fontsize=10)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)
    
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
        all_dates = extract_dates_from_headers(df_dept)
        selected_dates = select_period(all_dates, start_date_str, end_date_str)
        
        if not selected_dates:
            raise Exception("Не найдено данных за указанный период")
        
        parsed_data = parse_department_sheet(df_dept, selected_dates)
        
        # Создаем временные файлы
        temp_files = []
        temp_dir = tempfile.mkdtemp()
        
        for indicator, shops_data in parsed_data.items():
            safe_indicator_name = "".join(c for c in indicator if c.isalnum() or c in (' ', '-', '_')).rstrip()
            output_path = os.path.join(temp_dir, f"{dept_name}_{safe_indicator_name}.png")
            date_labels = [f"{d[0].strftime('%d.%m')}-{d[1].strftime('%d.%m')}" for d in selected_dates]
            plot_indicator_trend(shops_data, indicator, output_path, date_labels)
            temp_files.append(output_path)
        
        return temp_files
        
    except Exception as e:
        raise Exception(f"Ошибка при генерации графиков: {str(e)}")
