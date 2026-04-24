import urllib.request
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("LoanPrediction").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

DATA_URL = (
    "https://raw.githubusercontent.com/learningtechnologieslab/mds_cloud_computing"
    "/refs/heads/main/apache_spark/loan_data.csv"
)

# Spark's Hadoop HTTP filesystem doesn't support listStatus, so we download
# the file locally first and read from the local path instead.
local_csv = os.path.join(tempfile.gettempdir(), "loan_data.csv")
urllib.request.urlretrieve(DATA_URL, local_csv)

df = spark.read.csv(local_csv, header=True, inferSchema=True)

# --- 1. MISSING VALUES ---
#
# Strategy:
#   - Categorical columns (Gender, Married, Dependents, Self_Employed):
#     fill with the mode (most frequent value). These are nominal; the mode
#     preserves the existing distribution without introducing an artificial
#     category like "Unknown", which would then require special handling later.
#   - LoanAmount: fill with the median. Loan amounts are right-skewed, so the
#     median is more representative than the mean and is robust to outliers.
#   - Loan_Amount_Term: fill with the mode. The vast majority of loans are
#     360-month terms, so the mode reflects the realistic default.
#   - Credit_History: fill with the mode (1.0). Missing credit history is
#     ambiguous; using the mode avoids falsely penalising applicants.

def mode(df, col):
    return (
        df.groupBy(col)
        .count()
        .orderBy(F.desc("count"))
        .first()[0]
    )

cat_fill = {
    "Gender": mode(df, "Gender"),
    "Married": mode(df, "Married"),
    "Dependents": mode(df, "Dependents"),
    "Self_Employed": mode(df, "Self_Employed"),
    "Loan_Amount_Term": mode(df, "Loan_Amount_Term"),
    "Credit_History": mode(df, "Credit_History"),
}

loan_amount_median = df.approxQuantile("LoanAmount", [0.5], 0.001)[0]

df = df.fillna(cat_fill)
df = df.fillna({"LoanAmount": loan_amount_median})

# --- 2. OUTLIER DETECTION & TREATMENT ---
#
# Keep outliers rather than drop them. Loan data naturally contains high
# earners and large loan amounts that are genuine observations, not data errors.
# Dropping them would reduce the training set size (already ~600 rows) and
# could bias the model against high-income applicants. Instead, the
# discretisation step in section 3 will group extreme values into a top bucket,
# which effectively limits their influence without discarding information.
#
# For reference, IQR-based detection on ApplicantIncome shows ~100 values above
# the upper fence (Q3 + 1.5*IQR), confirming these are real high earners rather
# than data entry errors.

# --- 3. DISCRETISATION ---
#
# ApplicantIncome  → 4 bins: Low / Medium / High / Very High
#   Breakpoints chosen at roughly the 25th, 50th, and 75th percentiles so each
#   bucket has a comparable number of applicants.  Very High (>10,000) captures
#   the outlier region identified above.
#
# CoapplicantIncome → 3 bins: None / Low / High
#   A large share of applicants have zero co-applicant income, so "None" (0)
#   is its own bucket.  Above zero we split at ~2,500 (near the median of
#   non-zero values) into Low and High.
#
# LoanAmount → 3 bins: Small / Medium / Large
#   Breakpoints at 100 and 200 (thousands) align with natural product tiers
#   (personal / home improvement / home purchase).
#
# Loan_Amount_Term → 3 bins: Short / Medium / Long
#   ≤180 months (≤15 years) = Short; 181–360 = Medium (the dominant group);
#   >360 = Long.  This captures the step-function distribution of term values.

df = df.withColumn(
    "ApplicantIncome_bin",
    F.when(F.col("ApplicantIncome") <= 2500, "Low")
     .when(F.col("ApplicantIncome") <= 4500, "Medium")
     .when(F.col("ApplicantIncome") <= 10000, "High")
     .otherwise("Very High"),
)

df = df.withColumn(
    "CoapplicantIncome_bin",
    F.when(F.col("CoapplicantIncome") == 0, "None")
     .when(F.col("CoapplicantIncome") <= 2500, "Low")
     .otherwise("High"),
)

df = df.withColumn(
    "LoanAmount_bin",
    F.when(F.col("LoanAmount") <= 100, "Small")
     .when(F.col("LoanAmount") <= 200, "Medium")
     .otherwise("Large"),
)

df = df.withColumn(
    "Loan_Amount_Term_bin",
    F.when(F.col("Loan_Amount_Term") <= 180, "Short")
     .when(F.col("Loan_Amount_Term") <= 360, "Medium")
     .otherwise("Long"),
)

# -- 4. FEATURE SELECTION ---
#
# EXCLUDED:
#   Loan_ID  – a surrogate key with no predictive signal.
#   ApplicantIncome, CoapplicantIncome, LoanAmount, Loan_Amount_Term –
#     replaced by their discretised versions to reduce noise.
#
# INCLUDED and reasoning:
#   Gender          – lenders have historically considered gender; keeps model
#                     realistic (though fairness-aware evaluation is advisable).
#   Married         – two-income households have lower default rates.
#   Dependents      – more dependents increase financial burden.
#   Education       – graduates typically have higher and more stable income.
#   Self_Employed   – self-employment introduces income volatility.
#   Credit_History  – strongest single predictor of loan repayment in practice.
#   Property_Area   – property location correlates with collateral value and
#                     local economic conditions.
#   ApplicantIncome_bin      – income level after discretisation.
#   CoapplicantIncome_bin    – co-applicant contribution after discretisation.
#   LoanAmount_bin           – loan size category after discretisation.
#   Loan_Amount_Term_bin     – repayment horizon after discretisation.

categorical_features = [
    "Gender",
    "Married",
    "Dependents",
    "Education",
    "Self_Employed",
    "Property_Area",
    "ApplicantIncome_bin",
    "CoapplicantIncome_bin",
    "LoanAmount_bin",
    "Loan_Amount_Term_bin",
]

numeric_features = ["Credit_History"]

label_col = "Loan_Status"

# --- 5. LABEL ENCODING ---
#
# StringIndexer maps each distinct string value to an integer index ordered by
# frequency (most frequent = 0). This satisfies MLlib's requirement for numeric
# input without implying any ordinal relationship between categories.

indexers = [
    StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep")
    for c in categorical_features + [label_col]
]

feature_cols = [c + "_idx" for c in categorical_features] + numeric_features

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# --- 6. TRAIN / TEST SPLIT ---

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# --- 7. CLASSIFIERS ---

evaluator = MulticlassClassificationEvaluator(
    labelCol="Loan_Status_idx",
    predictionCol="prediction",
    metricName="accuracy",
)

def build_and_evaluate(classifier, name):
    pipeline = Pipeline(stages=indexers + [assembler, classifier])
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)
    accuracy = evaluator.evaluate(predictions)
    print(f"{name} Accuracy: {accuracy:.4f}")
    return name, accuracy

# Model 1 – Logistic Regression
# A strong linear baseline; fast to train and interpretable coefficients make
# it a natural first choice for binary classification problems.
lr = LogisticRegression(
    labelCol="Loan_Status_idx",
    featuresCol="features",
    maxIter=100,
)
lr_name, lr_acc = build_and_evaluate(lr, "Logistic Regression")

# Model 2 – Decision Tree
# Handles non-linear boundaries and feature interactions without scaling.
# A shallow tree (maxDepth=5) avoids overfitting on this small dataset.
dt = DecisionTreeClassifier(
    labelCol="Loan_Status_idx",
    featuresCol="features",
    maxDepth=5,
    seed=42,
)
dt_name, dt_acc = build_and_evaluate(dt, "Decision Tree")

# Model 3 – Random Forest
# An ensemble of trees that reduces variance compared to a single Decision Tree
# and generally outperforms both LR and DT on tabular data with mixed feature
# types. 100 trees with maxDepth=5 balances accuracy and training time.
rf = RandomForestClassifier(
    labelCol="Loan_Status_idx",
    featuresCol="features",
    numTrees=100,
    maxDepth=5,
    seed=42,
)
rf_name, rf_acc = build_and_evaluate(rf, "Random Forest")

# --- 8. MODEL COMPARISON ---
#
# All three models are evaluated on the same held-out 20 % split so comparisons
# are fair. Logistic Regression serves as the interpretable baseline. If the
# Decision Tree outperforms it, the relationship between features and approval
# is likely non-linear. Random Forest is expected to be the strongest model
# because ensemble averaging reduces the high variance typical of decision trees
# trained on small datasets. Credit_History is the dominant feature in tree
# models; its near-zero missing-value rate after imputation means the imputation
# choice has little effect on final performance.

results = [
    (lr_name, lr_acc),
    (dt_name, dt_acc),
    (rf_name, rf_acc),
]

best_model = max(results, key=lambda x: x[1])
print(f"\nBest model: {best_model[0]} (accuracy {best_model[1]:.4f})")

# --- 9. WRITE OUTPUT FILE ---

output_path = "output.txt"
with open(output_path, "w") as f:
    f.write("Loan Approval Prediction – Model Accuracy Scores\n")
    f.write("=" * 50 + "\n")
    for name, acc in results:
        f.write(f"{name}: {acc:.4f}\n")
    f.write("-" * 50 + "\n")
    f.write(f"Best Model: {best_model[0]} ({best_model[1]:.4f})\n")

print(f"Results written to {output_path}")

spark.stop()
