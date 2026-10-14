import os

import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

INDEX_NAME = os.environ["VECTOR_INDEX_NAME"]
CHAT_MODEL = os.environ["CHAT_MODEL"]
TOP_K = 6

SYSTEM_PROMPT = (
    "You are an assistant for emergency room operations. Answer ONLY from the provided context. "
    "If the context does not contain the answer, say you don't know. "
    "Cite the doc_ids you used in square brackets, e.g. [dept-Orthopedics-2024-01]."
)

w = WorkspaceClient()


def retrieve(question: str) -> list[dict]:
    res = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=["doc_id", "text"],
        query_text=question,
        num_results=TOP_K,
    )
    return [{"doc_id": r[0], "text": r[1]} for r in (res.result.data_array or [])]


def answer(question: str, docs: list[dict]) -> str:
    context = "\n".join(f"[{d['doc_id']}] {d['text']}" for d in docs)
    resp = w.serving_endpoints.query(
        name=CHAT_MODEL,
        messages=[
            ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT),
            ChatMessage(role=ChatMessageRole.USER, content=f"Context:\n{context}\n\nQuestion: {question}"),
        ],
        temperature=0.1,
    )
    return resp.choices[0].message.content


st.title("ER Operations Knowledge Assistant")

if "history" not in st.session_state:
    st.session_state.history = []

for role, text in st.session_state.history:
    st.chat_message(role).write(text)

if question := st.chat_input("Ask about wait times, satisfaction, departments or doctors"):
    st.chat_message("user").write(question)
    docs = retrieve(question)
    reply = answer(question, docs)
    st.chat_message("assistant").write(reply)
    with st.expander("Sources"):
        for d in docs:
            st.write(f"**{d['doc_id']}**: {d['text']}")
    st.session_state.history += [("user", question), ("assistant", reply)]
