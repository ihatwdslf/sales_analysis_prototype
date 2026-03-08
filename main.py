import streamlit as st
import pandas as pd
import sqlalchemy.exc
import uuid
from sqlalchemy import create_engine, text
import json
import plotly.express as px
from prophet import Prophet

if 'analysis_done' not in st.session_state:
    st.session_state['analysis_done'] = False
if 'graph_df' not in st.session_state:
    st.session_state['graph_df'] = None

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
    - Reason
    - Status (наприклад "Втрачено", "Списано")
    - Notes

    **Будь-які інші колонки ігноруються**

    Назви колонок чутливі до регістру! Якщо чогось не вистачає — програма видасть помилку.
    """)
    st.info("Завантажте файл і натисніть 'Виконати аналіз'")

uploaded_file = st.file_uploader(
    "Оберіть файл (.xlsx)",
    type=["xlsx", "xls"])

if uploaded_file is not None:
    file_name = uploaded_file.name.lower()
    if not file_name.endswith(('.xlsx', '.xls')):
        st.error("Непідтримуваний формат файлу. Підтримуються .xlsx, .xls")
        st.stop()

    for key in list(st.session_state.keys()):
        if key in ['processed_df', 'df']:
            del st.session_state[key]
    try:
        df = pd.read_excel(uploaded_file, engine='openpyxl')

        # Перевірка на дублікати
        duplicated = df.columns[df.columns.duplicated(keep=False)].tolist()
        if duplicated:
            st.error("Дублікати колонок у файлі: " + str(duplicated))
            st.stop()
        else:
            st.success("Колонки унікальні — ок!")

        # Валідація обов'язкових колонок
        required_cols = ['Product Name', 'Date', 'Quantity', 'Unit Cost']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            st.error(f"Відсутні обов'язкові колонки: {', '.join(missing)}")
            st.stop()

        # Попередження про зайві колонки
        all_expected = required_cols + ['Category', 'Reason', 'Notes', 'Status']
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
                    f"Колонка '{col}' містить порожні значення. Усі клітинки мають бути заповнені.")
                st.stop()

        # Перейменовуємо тільки існуючі колонки
        rename_dict = {
            'Product Name': 'product_name',
            'Category': 'category',
            'Date': 'date',
            'Quantity': 'quantity',
            'Unit Cost': 'unit_cost',
            'Status': 'status',
            'Reason': 'reason',
            'Notes': 'notes'
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

    # Кнопка Виконати аналіз
    if 'df' in st.session_state:
        df = st.session_state['df']

        if st.button("Виконати аналіз"):
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
                            res = conn.execute(text("SELECT product_id FROM Products WHERE name = :name"), {"name": name})
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
                        columns_for_losses = {
                            'product_id': df['product_id'],
                            'upload_id': df['upload_id'],
                            'date': df['date'],
                            'quantity': df['quantity'],
                            'total_cost': df['total_cost'],
                            'reason': df.get('reason', pd.Series([None] * len(df))),
                            'status': df.get('status', pd.Series([None] * len(df))),
                            'notes': df.get('notes', pd.Series([None] * len(df)))
                        }

                        for col in ['status']:
                            if col in df.columns:
                                columns_for_losses[col] = df[col]
                            else:
                                columns_for_losses[col] = pd.Series([None] * len(df))

                        losses_df = pd.DataFrame(columns_for_losses)

                        losses_df.to_sql(
                            'losses',
                            con=conn,
                            if_exists='append',
                            index=False,
                            method='multi'
                        )

                    st.success(f"Аналіз виконано! Дані збережено та графіки готові.")
                    st.info(f"Додано нових товарів: {added_products}")
                    st.info(f"Збережено рядків втрат: {len(losses_df)}")

                    # Зберігаємо df для графіків
                    st.session_state['graph_df'] = df.copy()

                    # Графіки
                    if st.session_state.get('analysis_done', False):
                        df = st.session_state['graph_df']

                    # Графіки
                    st.subheader("Результати аналізу")
                    blue_palette = ['#1E88E5', '#42A5F5', '#64B5F6', '#90CAF9', '#BBDEFB', '#E3F2FD']

                    if 'date' in df.columns and 'quantity' in df.columns:
                        by_date = df.groupby('date')['quantity'].sum().reset_index()
                        fig1 = px.line(
                            by_date,
                            x='date',
                            y='quantity',
                            title='Втрати по днях',
                            markers=True,
                            color_discrete_sequence=['#1E88E5']
                        )
                        fig1.update_layout(xaxis_title="Дата", yaxis_title="Кількість втрат")
                        st.plotly_chart(fig1, width='stretch')

                    # 2. Топ-продукти (стовпчиковий)
                    if 'product_name' in df.columns and 'quantity' in df.columns:
                        by_product = df.groupby('product_name')['quantity'].sum().reset_index()
                        by_product = by_product.sort_values('quantity', ascending=False).head(10)
                        fig2 = px.bar(
                            by_product,
                            x='product_name',
                            y='quantity',
                            title='Топ-10 продуктів за втратами',
                            color='quantity',
                            color_continuous_scale='Blues'
                        )
                        fig2.update_layout(xaxis_title="Товар", yaxis_title="Кількість втрат", xaxis_tickangle=-45)
                        st.plotly_chart(fig2, width='stretch')

                    # 3. За статусом (пиріг)
                    if 'status' in df.columns:
                        status_counts = df['status'].value_counts().reset_index()
                        status_counts.columns = ['status', 'count']
                        fig3 = px.pie(
                            status_counts,
                            values='count',
                            names='status',
                            title='Розподіл за статусом',
                            hole=0.4,
                            color_discrete_sequence=blue_palette
                        )
                        st.plotly_chart(fig3, width='stretch')

                    # 4. За категорією (якщо є category)
                    if 'category' in df.columns and 'quantity' in df.columns:
                        by_category = df.groupby('category')['quantity'].sum().reset_index()
                        fig4 = px.bar(
                            by_category,
                            x='category',
                            y='quantity',
                            title='Втрати по категоріях',
                            color='quantity',
                            color_continuous_scale='Blues'
                        )
                        fig4.update_layout(xaxis_title="Категорія", yaxis_title="Кількість втрат")
                        st.plotly_chart(fig4, width='stretch')

                    # 5. Топ-причини втрат
                    if 'reason' in df.columns and 'quantity' in df.columns:
                        by_reason = df.groupby('reason')['quantity'].sum().reset_index()
                        by_reason = by_reason.sort_values('quantity', ascending=False).head(10)
                        if not by_reason.empty:
                            fig5 = px.bar(
                                by_reason,
                                x='reason',
                                y='quantity',
                                title='Топ-причини втрат',
                                color='quantity',
                                color_continuous_scale='Blues'
                            )
                            fig5.update_layout(xaxis_title="Причина", yaxis_title="Кількість втрат",
                                               xaxis_tickangle=-45)
                            st.plotly_chart(fig5, width='stretch')

                    # Кнопка прогнозу
                    st.subheader("Прогноз на 30 днів вперед")
                    if 'date' in df.columns and 'quantity' in df.columns:
                        try:
                            prophet_df = df[['date', 'quantity']].copy()
                            prophet_df = prophet_df.rename(columns={'date': 'ds', 'quantity': 'y'})
                            prophet_df = prophet_df.groupby('ds')['y'].sum().reset_index()
                            prophet_df = prophet_df.dropna()

                            if len(prophet_df) < 2:
                                st.info("Недостатньо даних для прогнозу (потрібно мінімум 2 дати).")
                            elif len(prophet_df) < 7:
                                st.info("Даних мало для надійного прогнозу (рекомендовано мінімум 7 дат).")
                            else:
                                model = Prophet(
                                    yearly_seasonality=False,
                                    weekly_seasonality=True,
                                    daily_seasonality=True
                                )
                                model.fit(prophet_df)

                                future = model.make_future_dataframe(periods=30)
                                forecast = model.predict(future)

                                # Основний графік прогнозу (реальні дані + прогноз)
                                fig_forecast = model.plot(forecast)

                                ax = fig_forecast.gca()
                                ax.set_title("  ")
                                ax.set_title("Прогноз кількості втрат на 30 днів вперед", color='#FFFFFF', fontsize=12, fontweight='bold')
                                ax.set_xlabel("Дата", color='#FFFFFF', fontsize=14)
                                ax.set_ylabel("Кількість втрат", color='#FFFFFF', fontsize=14)
                                for label in ax.get_xticklabels() + ax.get_yticklabels():
                                    label.set_color('#FFFFFF')

                                ax.tick_params(axis='both', which='major', labelsize=12)
                                ax.lines[0].set_markerfacecolor('#FFFFFF')
                                ax.lines[0].set_markeredgecolor('#FFFFFF')
                                ax.lines[0].set_markersize(6)

                                st.plotly_chart(fig_forecast, width='stretch')

                                # Текстовий висновок
                                last_date = forecast['ds'].max()
                                predicted = forecast[forecast['ds'] == last_date]['yhat'].values[0]
                                st.info(f"Прогноз на {last_date.strftime('%Y-%m-%d')}: очікувана кількість ≈ **{predicted:.0f}** (враховує тренд та сезонність за даними).")

                                st.success("Прогноз побудовано!")
                        except Exception as e:
                            st.error("Помилка прогнозу:")
                            st.error(str(e))

                except Exception as e:
                    st.error("Помилка при збереженні:")
                    st.error(str(e))

st.markdown("---")
st.caption("Прототип курсової | Streamlit + PostgreSQL (Docker)")

