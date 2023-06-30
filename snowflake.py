# Snowpark
import joblib
# Pandas & json
import pandas as pd
from sklearn import metrics
from sklearn.ensemble import RandomForestRegressor
# Models
from sklearn.model_selection import train_test_split
from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import pandas_udf
from snowflake.snowpark.types import *
from snowflake.snowpark.version import VERSION
import snowflake.snowpark as snowpark
from tabulate import tabulate

def main(session: snowpark.Session):
    snowpark_version = VERSION
    print('Database                    : {}'.format(session.get_current_database()))
    print('Schema                      : {}'.format(session.get_current_schema()))
    print('Warehouse                   : {}'.format(session.get_current_warehouse()))
    print('Role                        : {}'.format(session.get_current_role()))
    print('Snowpark for Python version : {}.{}.{}'.format(snowpark_version[0],snowpark_version[1],snowpark_version[2]))
    print()

    # 파일포맷 생성 및 스테이지 에서 테이블로 데이터 로드
    session.sql('''CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1;''').collect()
    session.sql("COPY INTO CUSTOMERS FROM @TEST/EcommerceCustomers FILE_FORMAT = (FORMAT_NAME = my_csv_format)").collect()
    custdf = session.table('customers').toPandas()
    
    # 머신러닝의 입력 컬럼과 출력 컬럼 설정
    X = custdf[['SESSION_LENGTH', 'TIME_ON_APP',
           'TIME_ON_WEBSITE', 'LENGTH_OF_MEMBERSHIP']]
    Y = custdf['YEARLY_AMOUNT_SPENT']

    # 데이터 셋 설정
    X_train, X_test, y_train, y_test = train_test_split(X, Y,
                                 test_size=0.3, random_state=101)
    session.sql("CREATE STAGE IF NOT EXISTS PCS_models").collect()

    # 선형회귀 분석으로 데이터 학습
    rf = RandomForestRegressor()
    rf.fit(X_train,y_train)

    # 학습된 모델 파일 스테이지에 로드
    joblib.dump(rf,"/tmp/prc_model.joblib")
    session.file.put("/tmp/prc_model.joblib", "@PCS_models", auto_compress=True, overwrite=True)
    
    # 학습된 모델로 데이터 예측
    def predict_pandas_udf(df: pd.DataFrame) -> pd.Series:
        return pd.Series(rf.predict(df))  
    
    linear_model_vec = pandas_udf(func=predict_pandas_udf,
                                    return_type=FloatType(),
                                    input_types=[FloatType(),FloatType(),FloatType(),FloatType()])

    # 결과 데이터 테이블에 로드
    snow_output = session.table('CUSTOMERS').select(*list(X.columns),
                    linear_model_vec(list(X.columns)).alias('PREDICTED_SPEND'), 
                    (F.col('YEARLY_AMOUNT_SPENT')).alias('ACTUAL_SPEND')
                    )
    
    snow_output.write.mode("overwrite").saveAsTable("PREDICTED_CUSTOMER_SPEND")

    # 결정계수 확인
    pd_output=snow_output.toPandas()
    
    y1 = pd_output["ACTUAL_SPEND"]
    y2 = pd_output["PREDICTED_SPEND"]
    
    r2 = metrics.r2_score(y_pred=y2, y_true=y1)
    print()
    print("Coefficient of Determination")
    print(round(r2, 2))
    
    # 특성 중요도 확인
    coeff_df = pd.DataFrame(rf.feature_importances_,X.columns,columns=['Feature Importance']).round(2)
    print()
    print(tabulate(coeff_df, headers="keys", stralign="left", tablefmt="rst"))
    print()
    coeff_df = session.createDataFrame(coeff_df)
    
    return snow_output