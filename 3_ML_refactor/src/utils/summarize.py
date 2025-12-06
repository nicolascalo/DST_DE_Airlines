import pandas as pd
import os
from config.config_ml import SETTINGS_ML

def save_global_summary_and_clean_models(global_summary, SETTINGS_ML,best_model_scores):

    previous_best_models =  pd.DataFrame(best_model_scores['df_best_models'])


    df = pd.DataFrame(global_summary)

    df = pd.concat([df,previous_best_models])

    out_dir = os.path.join(SETTINGS_ML['OUTPUT_DIR'], SETTINGS_ML['RUN_TIMESTAMP'])
    os.makedirs(out_dir, exist_ok=True)


    try:
        df = df.sort_values(['accuracy','r2']).drop_duplicates(["problem_type"], keep="last")

        df.to_csv(f"{SETTINGS_ML['OUTPUT_DIR']}/best_models/best_models.csv",index=0)
        model_files_to_delete = sorted([f for f in 
        
        list(os.listdir(f"{SETTINGS_ML['OUTPUT_DIR']}/best_models/")) 
        if ('.pkl' in f) 
        & (f not in set(df['filename']))]
        )


        for file in model_files_to_delete:
            os.remove(f"{SETTINGS_ML['OUTPUT_DIR']}/best_models/{file}")

    except:
        
        pd.DataFrame(global_summary).to_csv(f"{SETTINGS_ML['OUTPUT_DIR']}/best_models/best_models.csv",index=0)

