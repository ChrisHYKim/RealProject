import streamlit as st
from langchain_core.prompts import PromptTemplate

# from langchain_aws import ChatBedrock
# from langchain_aws import ChatBedrockConverse
# from langchain_aws.llms.bedrock import BedrockLLM
# from langchain_community.llms.amazon_api_gateway import AmazonAPIGateway
# from langchain_aws import SagemakerEndpoint
from langchain_core.output_parsers import StrOutputParser
from langchain_ollama import ChatOllama
from backend import process_data
import asyncio

st.title("RealEstate Chat")

if "information" not in st.session_state:
    st.session_state.information = None  # 초기화


async def run_process():
    information = await process_data()
    if information:
        st.session_state.information = information
        st.success("정보가 준비되었습니다.")
    else:
        st.error("정보를 찾을 수 없습니다.")


# 질문 처리 함수
async def process_question(question):
    await run_process()

    information = st.session_state.get("information")
    if information:
        try:
            llm = ChatOllama(model="mistral", temperature=0)
            template = """
                    Question: {question}
                    Information: {information}
                """
            prompt = PromptTemplate(
                template=template, input_variables=["question", "information"]
            )
            chain = prompt | llm
            result = await chain.ainvoke(
                {"question": question, "information": information}
            )
            # 비동기 호출
            data = result["text"]
            with st.chat_message("ai"):
                st.markdown(data)
        except Exception as e:
            st.error(f"LLM 처리 중 오류 발생: {e}")
    else:
        st.info("정보를 먼저 요청해주세요.")


question = st.chat_input("질문 사항 입력해라")
if question:
    with st.chat_message("You"):
        st.markdown(question)
    asyncio.run(process_question(question))
