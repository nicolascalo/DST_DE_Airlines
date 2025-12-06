def get_numeric_categorical(df, target_col):
    numeric = df.select_dtypes(include=['number']).columns.tolist()
    if target_col in numeric:
        numeric.remove(target_col)
    categorical = df.select_dtypes(include=['object', 'category']).columns.tolist()
    if target_col in categorical:
        categorical.remove(target_col)
    return numeric, categorical
