# Overview:
다음은 Scikit-Learn, Snowpark 및 Python UDF에서 선형 회귀 모델을 사용하여 고객 지출을 예측하는 예입니다. 모델 교육은 로컬에서 수행되며 점수는 Jupyter Notebook의 Snowpark를 통해 생성된 UDF를 사용하여 Snowflake 내에서 수행됩니다. 샘플 데이터 파일은 EcommerceCustomers입니다. 모델 교육은 테스트 데이터 세트에서 수행되며 점수는 전체 데이터 세트에서 수행됩니다.


전자상거래 소매업체는 기계 학습을 사용하여 디지털 아울렛(웹 사이트 및 앱)에 대한 고객의 온라인 참여를 이해하려고 합니다. 이 회사는 모바일 앱의 경험과 웹사이트 중 어느 것에 집중할지 결정하려고 한다. 선형 회귀 모델을 사용하여 어떤 사용자의 활동이 더 많은 돈을 사용할 가능성에 가장 큰 영향을 미치는지 확인합니다.

변수:

평균 세션 길이: 세션의 평균 세션 시간.
앱 사용 시간: 앱 평균 사용 시간(분)
웹 사이트 체류 시간: 웹 사이트에서 보낸 평균 시간(분)
회원 기간: 고객이 회원이 된 이후의 년수

# Prerequisites:
Snowpark for Python library v.06

* Snowflake account
* Snowpark for Python
* The examples also use the following Python libraries:
   ```
   scikit-learn
   pandas
   numpy
   matplotlib
   seaborn
   streamlit
   ```
* Jupyter or JupyterLab
If any of the packages used in the example are not part of your python environment, you can install them using
<br>`import sys`<br>
`!conda install --yes --prefix {sys.prefix} <package_name>`
* Latest streamlit package, which you can get by
 `!pip install streamlit`

## What You'll Learn:
Snowpark의 간단한 입문 자습서. 데이터 캡처, 데이터 과학 및 Streamlit을 사용하여 앱 만들기를 다룹니다.

* 특징 엔지니어링을 위해 Python 용 Snowpark를 사용하는 방법
* Snowflake 외부에서 기계 학습 모델을 교육하고 Python UDF로 배포
* 최종 사용자 앱에서 작업 모델 시각화


# Usage/Steps

세 가지 회귀 방법을 비교하고 최적의 회귀 방법을 구현하는이 Hex 노트북 / 앱을 참조하십시오. 고객 지출 예측 - 회귀 (https://app.hex.tech/snowflake/app/ 5f29ae86-949b-40cf-bbe0-152037d0d9ef/) 최신)


![hexapp]


또는 로컬에서 실행하려면,

1. 터미널을 열고 이 리포지토리를 복제하거나 GitHub 데스크톱을 사용합니다. 이는 Snowflakecorp 조직의 일부이므로 복제하기 전에 인증을 설정해야 합니다.

      "git clone https://github.com/Snowflake-Labs/snowpark-python-demos"

2. Predict Customer Spend 디렉토리로 이동하여 JupyterLab을 시작합니다.

      「쥬피터 랩」

3. 브라우저 창에 URL을 붙여넣고 JupyterLab이 시작되면 작업 디렉토리로 전환하여 creds_generic.json을 업데이트하고 눈송이 환경을 반영하도록 이름을 creds.json으로 변경합니다.

4. streamlit (ecommapp)을 실행하려면 터미널에서 streamlit run ecommapp.py를 실행합니다.
     앱은 다음과 같습니다.

![ecommapp](https://user-images.githubusercontent.com/1723932/179316941-87b298f2-43de-4635-a0b1-bdc68f059605.png)
