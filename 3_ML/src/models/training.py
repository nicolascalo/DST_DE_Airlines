import pickle
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import classification_report, ConfusionMatrixDisplay, mean_absolute_error, mean_squared_error, r2_score, accuracy_score, recall_score,f1_score,precision_score
from src.utils.logger import get_logger
from src.models.feature_importance import save_feature_importance
from config.config_ml import SETTINGS_ML
import json
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os



def sklearn_params_to_dict(params):
    """
    Recursively convert sklearn parameters to JSON-serializable dict.
    """
    serializable = {}
    for k, v in params.items():
        try:
            serializable[k] = v.get_params() if hasattr(v, 'get_params') else v
        except:
            serializable[k] = str(v)  # fallback
    return serializable


def extract_hyperparameters(pipe):

    if hasattr(pipe, 'best_estimator_'):
        model = pipe.best_estimator_.named_steps.get('classifier', pipe.best_estimator_)
    else:
        model = pipe.named_steps.get('classifier', pipe)
    hyperparams = json.dumps(sklearn_params_to_dict(model.get_params()))
    return hyperparams
    


def run_all_pipelines(pipelines, datasets, SETTINGS_ML, best_model_scores):

    logger = get_logger()

    run_summary = pd.DataFrame()
    try:
        
        global_summary = pd.DataFrame(best_model_scores['df_best_models'])
    except:
        global_summary = pd.DataFrame(run_summary)



    for problem_type in ['classification_status','classification_delay','regression']:
        score_to_beat = best_model_scores[problem_type]
        logger.info(f"================================== {problem_type} training ==================================")

        for name, (pipe, params) in pipelines[problem_type].items():
            logger.info(f'Starting {problem_type} pipeline {name}')
            logger.info(f"{problem_type}_{SETTINGS_ML['RUN_TIMESTAMP']}_{name}.pkl")


            X_train = datasets[problem_type]['X_train']
            y_train = datasets[problem_type]['y_train']
            X_test = datasets[problem_type]['X_test']
            y_test = datasets[problem_type]['y_test']

            if "classification" in problem_type :
                scoring_method = "accuracy"
            else:
                scoring_method = "r2"

            if SETTINGS_ML['RUN_MODE'] in ['grid','both'] and params:

                logger.info(f"Performing GridSearchCV (cv = {SETTINGS_ML['CV_NB']}, scoring=scoring_method) for {name} with parameters: {params}")
                
                gs = GridSearchCV(pipe, params, cv=SETTINGS_ML['CV_NB'], scoring=scoring_method, n_jobs=SETTINGS_ML['PARALLEL_JOBS'])
                gs.fit(X_train, y_train)
                pipe_used = gs
            else:
                logger.info(f"Fitting simple pipeline for {problem_type} {name}")
                
                pipe.fit(X_train, y_train)
                pipe_used = pipe

            y_pred = pipe_used.predict(X_test)



            if "classification" in problem_type :

                # Confusion matrix
                f, ax = plt.subplots(figsize=(10,10))
                ConfusionMatrixDisplay.from_predictions(y_test, y_pred, ax=ax)
                plt.savefig(f"{SETTINGS_ML['OUTPUT_DIR']}/training_runs/{SETTINGS_ML['RUN_TIMESTAMP']}/{problem_type}/confusion_matrix/{SETTINGS_ML['RUN_TIMESTAMP']}_{name}_{SETTINGS_ML['RUN_MODE']}_confusion_matrix.png")
                plt.close()

                score = accuracy_score(y_test, y_pred)

                metrics = {
                "accuracy" : accuracy_score(y_test, y_pred),
                "macro_avg_recall" : recall_score(y_test, y_pred, average='macro'),
                "macro_avg_precision" : precision_score(y_test, y_pred, average='macro'),
                "macro_avg_f1" : f1_score(y_test, y_pred, average='macro'),
                "target_values": set(y_train.values)
                }

                logger.info('%s accuracy: %.3f', name, score)

            else:

                score = r2_score(y_test, y_pred)

                metrics = {
                    'mae': mean_absolute_error(y_test, y_pred),
                    'mse': mean_squared_error(y_test, y_pred),
                    'rmse': np.sqrt(mean_squared_error(y_test, y_pred)),
                    'r2': r2_score(y_test, y_pred)
                }
                            
                logger.info(f'{name} r2: {score}')
            

            
            
            current_summary = pd.DataFrame([{
                'filename':f"{problem_type}_{SETTINGS_ML['RUN_TIMESTAMP']}_{name}.pkl",
                'run': SETTINGS_ML['RUN_TIMESTAMP'],
                'pipeline': name,
                'mode': SETTINGS_ML['RUN_MODE'],
                'problem_type': problem_type,
                'dataset_size_training': len(X_train),
                'dataset_size_testing': len(X_test),
                **metrics,
                'target': y_test.name,
                'features': X_test.columns.to_series().groupby(X_test.dtypes).groups,
                'hyperparameters': extract_hyperparameters(pipe)

            }])


            run_summary = pd.concat([run_summary,current_summary])


            save_feature_importance(pipe_used, name, problem_type, SETTINGS_ML)

            # save model
            if score_to_beat < score:
                logger.info(f'Better than previous: {score_to_beat}, new {score}')



                score_to_beat = score

                with open(f"{SETTINGS_ML['OUTPUT_DIR']}/best_models/{problem_type}_{SETTINGS_ML['RUN_TIMESTAMP']}_{name}.pkl", 'wb') as f:
                    pickle.dump(pipe_used, f)


                try:
                    logger.info("OK")
                    global_summary = global_summary[global_summary['problem_type'] != problem_type]
                    global_summary = pd.concat([global_summary,run_summary])
                except:
                    logger.info("except")
                    global_summary = run_summary
            else:
                logger.info('Not better than previous')
    

    out_dir = os.path.join(SETTINGS_ML['OUTPUT_DIR'], "training_runs",SETTINGS_ML['RUN_TIMESTAMP'])
    os.makedirs(out_dir, exist_ok=True)
    run_summary.to_csv(os.path.join(out_dir, f"{SETTINGS_ML['RUN_TIMESTAMP']}_run_ml_summary.csv"), index=False)


    return global_summary
