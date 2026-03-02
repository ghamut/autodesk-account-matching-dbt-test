def model(dbt, session):
    print("Hello, world!")
    return dbt.ref('raw_pos_enr_250')
    