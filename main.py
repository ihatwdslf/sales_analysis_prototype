import streamlit as st
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:25071996@localhost:5432/postgres')

try:
    with engine.connect() as conn:
        st.success("Підключення до PostgreSQL (Docker) успішне!")
except Exception as e:
    st.error(f"Помилка: {e}")
st.title("Hello, World!")