from datetime import date

from transformations.src.department_monthly_stats import transform_department_monthly_stats
from transformations.src.doctor_performance import transform_doctor_performance
from transformations.src.doctors_silver import DOCTOR_VALIDATIONS, transform_doctors_silver
from transformations.src.wait_time_by_hour_weekday import transform_wait_time_by_hour_weekday
from transformations.src.wait_vs_satisfaction import transform_wait_vs_satisfaction

visits_schema = (
    "doctor_id string, doctor_first_name string, doctor_last_name string, department string, "
    "subspecialty string, university string, years_of_experience int, "
    "department_referral string, admission_month date, weekday_num int, weekday string, "
    "admission_hour int, wait_bucket_in_mins string, wait_time int, satisfaction_score int, "
    "admission_flag boolean, coordinator_involved int, age int"
)


def _visit(doctor="DOC-1", dept="Cardiology", wait=10, score=8, admitted=True, coord=1, age=40,
           bucket="00-15", hour=8):
    return (
        doctor, "Jane", "Doe", "Cardiology", "Heart", "MIT", 10,
        dept, date(2024, 1, 1), 2, "Mon", hour, bucket, wait, score, admitted, coord, age,
    )


# Doctors silver

def test_doctors_silver_cleans_text(spark):
    df = spark.createDataFrame(
        [(" doc-101 ", " ethan ", "CLOVER", " general practice ")],
        "doctor_id string, doctor_first_name string, doctor_last_name string, department string",
    )
    row = transform_doctors_silver(df).collect()[0]
    assert row.doctor_id == "DOC-101"
    assert row.doctor_first_name == "Ethan"
    assert row.doctor_last_name == "Clover"
    assert row.department == "General Practice"


def test_doctor_validations_accept_and_reject(spark):
    df = spark.createDataFrame(
        [("DOC-1", "Doe", 5), ("BAD", "Doe", 5), ("DOC-2", None, 5), ("DOC-3", "Doe", -1)],
        "doctor_id string, doctor_last_name string, years_of_experience int",
    )
    for name, rule in DOCTOR_VALIDATIONS.items():
        bad = {r.doctor_id for r in df.where(f"NOT ({rule})").collect()}
        expected = {"doctor id format": {"BAD"}, "last name not null": {"DOC-2"},
                    "experience non-negative": {"DOC-3"}}[name]
        assert bad == expected, name


# Gold aggregations

def test_doctor_performance(spark):
    visits = spark.createDataFrame(
        [_visit(wait=10, score=8, admitted=True, coord=1),
         _visit(wait=30, score=None, admitted=False, coord=0)],
        visits_schema,
    )
    row = transform_doctor_performance(visits).collect()[0]
    assert row.visits == 2
    assert row.avg_wait_time == 20.0
    assert row.avg_satisfaction == 8.0          # NULL score is ignored by avg
    assert row.satisfaction_responses == 1
    assert row.admission_rate_pct == 50.0
    assert row.coordinator_usage_pct == 50.0


def test_department_monthly_stats_groups_by_department(spark):
    visits = spark.createDataFrame(
        [_visit(dept="Cardiology", age=20), _visit(dept="Cardiology", age=40), _visit(dept="Neurology")],
        visits_schema,
    )
    rows = {r.department_referral: r for r in transform_department_monthly_stats(visits).collect()}
    assert rows["Cardiology"].visits == 2
    assert rows["Cardiology"].avg_age == 30.0
    assert rows["Neurology"].visits == 1


def test_wait_time_by_hour_weekday(spark):
    visits = spark.createDataFrame(
        [_visit(wait=10, hour=8), _visit(wait=20, hour=8), _visit(wait=50, hour=22)],
        visits_schema,
    )
    rows = {r.admission_hour: r for r in transform_wait_time_by_hour_weekday(visits).collect()}
    assert rows[8].visits == 2
    assert rows[8].avg_wait_time == 15.0
    assert rows[22].avg_wait_time == 50.0


def test_wait_vs_satisfaction(spark):
    visits = spark.createDataFrame(
        [_visit(bucket="00-15", score=10), _visit(bucket="00-15", score=6),
         _visit(bucket="46+", score=2, admitted=False)],
        visits_schema,
    )
    rows = {r.wait_bucket_in_mins: r for r in transform_wait_vs_satisfaction(visits).collect()}
    assert rows["00-15"].avg_satisfaction == 8.0
    assert rows["46+"].avg_satisfaction == 2.0
    assert rows["46+"].admission_rate_pct == 0.0
