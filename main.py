import streamlit as st
import pandas as pd
import sqlalchemy.exc
import uuid
from sqlalchemy import create_engine, text
import json

DB_USER = "postgres"
DB_PASSWORD = "25071996"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

st.set_page_config(page_title="Аналіз продажів магазину", layout="wide")
st.title("Аналіз продажів продуктового магазину")

with st.sidebar:
    st.header("Вимоги до Excel-файлу")
    st.markdown("""
    **Обов'язкові колонки (точно так названі):**
    - Product Name
    - Date (наприклад 2025-02-15 або 15.02.2025)
    - Quantity (ціле число ≥ 0)
    - Unit Cost (дробове число, наприклад 28.50)

    **Необов'язкові колонки:**
    - Category
    - Product Description
    - Reason
    - Status (наприклад "Втрачено", "Списано")
    - Notes

    **Ігноруються повністю:**
    - Total Cost (рахуємо автоматично: Quantity × Unit Cost)
    - Будь-які інші колонки

    Назви колонок чутливі до регістру! Якщо чогось не вистачає — програма видасть помилку.
    """)
    st.info("Завантажте файл і натисніть 'Зберегти в базу'")

uploaded_file = st.file_uploader(
    "Оберіть файл (.xlsx)",
    type=["xlsx", "xls"]
)

if uploaded_file is not None:
        file_name = uploaded_file.name.lower()
        if not file_name.endswith(('.xlsx', '.xls')):
            st.error("Непідтримуваний формат файлу. Підтримуються .xlsx, .xls")
            st.stop()
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl')

            # # Дебаг: показуємо, що реально прочитано (можна видалити після тестів)
            # st.subheader("Колонки в файлі")
            # st.write(df.columns.tolist())

            # Перевірка на дублікати (має бути порожньо)
            duplicated = df.columns[df.columns.duplicated(keep=False)].tolist()
            if duplicated:
                st.error("Дублікати колонок у файлі: " + str(duplicated))
                st.stop()
            else:
                st.success("Колонки унікальні — ок!")

            # Валідація обов'язкових колонок (наявність)
            required_cols = ['Product Name', 'Date', 'Quantity', 'Unit Cost']
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                st.error(f"Відсутні обов'язкові колонки: {', '.join(missing)}")
                st.stop()

            # Попередження про зайві колонки
            all_expected = required_cols + ['Category', 'Product Description', 'Reason', 'Notes', 'Status']
            extra_cols = [col for col in df.columns if col not in all_expected]
            if extra_cols:
                st.warning(f"Колонки {', '.join(extra_cols)} не підтримуються і будуть проігноровані.")

            # Перетворюємо Date в datetime
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
            if df['Date'].isna().any():
                st.error("Деякі дати не розпізнано. Перевірте формат (наприклад 15.02.2025 або 2025-02-15).")
                st.stop()

            # Перевірка обов'язкових колонок на порожні значення
            for col in ['Product Name', 'Date']:
                if df[col].isna().any() or (df[col].astype(str).str.strip() == '').any():
                    st.error(
                        f"Колонка '{col}' містить порожні значення або пропуски. Усі клітинки мають бути заповнені.")
                    st.stop()

            # Перейменовуємо тільки існуючі колонки
            rename_dict = {
                'Product Name': 'product_name',
                'Category': 'category',
                'Date': 'date',
                'Quantity': 'quantity',
                'Unit Cost': 'unit_cost',
                'Status': 'status',
                'losses': 'losses_count'  # перейменували, щоб уникнути конфліктів
            }

            df = df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns})

            # Валідація після перейменування
            required = ['product_name', 'date', 'quantity', 'unit_cost']
            missing = [col for col in required if col not in df.columns]
            if missing:
                st.error(f"Відсутні обов'язкові після перейменування: {missing}")
                st.stop()

            # Валідація чисел
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
            df['unit_cost'] = pd.to_numeric(df['unit_cost'], errors='coerce')

            if df['quantity'].isna().any() or (df['quantity'] <= 0).any():
                st.error("Quantity: нечислові або ≤ 0")
                st.stop()

            if df['unit_cost'].isna().any() or (df['unit_cost'] <= 0).any():
                st.error("Unit Cost: нечислові або ≤ 0")
                st.stop()

            st.session_state['df'] = df.copy()

            st.success(f"Файл завантажено! Рядків: {len(df)}")
            st.subheader("Перегляд даних")
            st.dataframe(df.head(10))

        except Exception as e:
            st.error("Не вдалося прочитати або обробити файл:")
            st.error(str(e))

        # Кнопка збереження
        if 'df' in st.session_state:
            df = st.session_state['df']

            if st.button("Зберегти в базу даних"):
                with st.spinner("Обробка та збереження..."):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("DELETE FROM losses"))
                            conn.execute(text("DELETE FROM uploads"))
                            # 1. Створюємо запис в Uploads
                            metadata_dict = {
                                "rows": len(df),
                                "columns": len(df.columns),
                                "file_size_kb": round(uploaded_file.size / 1024, 1) if uploaded_file.size else 0
                            }
                            result = conn.execute(text("""
                                        INSERT INTO Uploads (file_name, upload_date, status, metadata)
                                        VALUES (:file_name, CURRENT_TIMESTAMP, 'processed', :metadata)
                                        RETURNING upload_id
                                    """), {
                                "file_name": uploaded_file.name,
                                "metadata": json.dumps(metadata_dict)
                            })
                            upload_id = result.fetchone()[0]

                            # 2. Унікальні продукти
                            unique_products = df[['product_name']].drop_duplicates(subset=['product_name'])

                            added_products = 0
                            product_map = {}

                            for _, row in unique_products.iterrows():
                                name = row['product_name']
                                # Перевіряємо чи існує
                                res = conn.execute(text("SELECT product_id FROM Products WHERE name = :name"),
                                                   {"name": name})
                                existing = res.fetchone()
                                if existing:
                                    product_map[name] = existing[0]
                                else:
                                    res = conn.execute(text("""
                                                INSERT INTO Products (name, category)
                                                VALUES (:name, :category)
                                                RETURNING product_id
                                            """), {
                                        "name": name,
                                        "category": row.get('category')
                                    })
                                    new_id = res.fetchone()[0]
                                    product_map[name] = new_id
                                    added_products += 1

                            # 3. Додаємо product_id і upload_id до df
                            df['product_id'] = df['product_name'].map(product_map)
                            df['upload_id'] = upload_id
                            df['total_cost'] = df['quantity'] * df['unit_cost']

                            # 4. Зберігаємо в Losses (тільки потрібні колонки)
                            losses_df = df[['product_id', 'upload_id', 'date', 'quantity', 'total_cost', 'status']]

                            losses_df.to_sql(
                                'losses',
                                con=conn,
                                if_exists='append',
                                index=False,
                                method='multi'
                            )

                        st.success(f"Успішно збережено!")
                        st.info(f"Додано нових товарів: {added_products}")
                        st.info(f"Збережено рядків втрат: {len(losses_df)}")
                        st.info(f"Upload ID: {upload_id} (використовується для розділення даних)")

                    except Exception as e:
                        st.error("Помилка при збереженні:")
                        st.error(str(e))
        # except Exception as e:
        #     st.error("Не вдалося прочитати або обробити файл:")
        #     st.error(str(e))


st.markdown("---")
st.caption("Прототип курсової | Streamlit + PostgreSQL (Docker)")

